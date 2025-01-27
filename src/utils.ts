import * as Y from 'yjs';
import * as encoding from 'lib0/encoding';
import * as decoding from 'lib0/decoding';
import { PgAdapter } from './pg-adapter';

const decodeStateVectorBuffer = (buffer: Uint8Array) => {
	let decoder;
	if (Buffer.isBuffer(buffer)) {
		decoder = decoding.createDecoder(buffer);
	} else if (Buffer.isBuffer(buffer?.buffer)) {
		decoder = decoding.createDecoder(buffer.buffer);
	} else {
		throw new Error('No buffer provided at decodeStateVectorBuffer()');
	}
	const clock = decoding.readVarUint(decoder);
	const sv = decoding.readVarUint8Array(decoder);
	return { sv, clock };
};

export const readStateVector = async (db: PgAdapter, docName: string) => {
	const svBuffer = await db.getStateVectorBuffer(docName);
	if (!svBuffer) {
		// no state vector created yet or no document exists
		return { sv: null, clock: -1 };
	}
	return decodeStateVectorBuffer(svBuffer);
};

/**
 * Update the state vector of a document in PostgreSQL.
 * @param db
 * @param docName
 * @param sv
 * @param clock is the latest id of the updates of the docName
 * @returns new state vector
 */
const writeStateVector = async (db: PgAdapter, docName: string, sv: Uint8Array, clock: number) => {
	const encoder = encoding.createEncoder();
	encoding.writeVarUint(encoder, clock);
	encoding.writeVarUint8Array(encoder, sv);
	const newSv = await db.putStateVector(docName, encoding.toUint8Array(encoder));
	return newSv;
};

export const getCurrentUpdateClock = async (db: PgAdapter, docName: string) =>
	db.findLatestDocumentId(docName);

/**
 * Store an update in PostgreSQL.
 * @param db
 * @param docName
 * @param update
 * @returns id of the stored document
 */
export const storeUpdate = async (db: PgAdapter, docName: string, update: Uint8Array) => {
	const clock = await getCurrentUpdateClock(db, docName);
	if (clock === -1) {
		// make sure that a state vector is always written, so we can search for available documents
		const ydoc = new Y.Doc();
		Y.applyUpdate(ydoc, update);
		const sv = Y.encodeStateVector(ydoc);
		await writeStateVector(db, docName, sv, 0);
	}

	const storedDoc = await db.insertUpdate(docName, update);
	return storedDoc.id;
};

/**
 * Merge all PostgreSQL records of the same yjs document together.
 */
export const flushDocument = async (
	db: PgAdapter,
	docName: string,
	stateAsUpdate: Uint8Array,
	stateVector: Uint8Array,
) => {
	const clock = await storeUpdate(db, docName, stateAsUpdate);
	await writeStateVector(db, docName, stateVector, clock);
	await db.clearUpdatesRange(docName, 0, clock);
	return clock;
};

export const getYDocFromDb = async (db: PgAdapter, docName: string, flushSize: number) => {
	console.log('[getYDocFromDb] start readAllUpdates');
	const updates: { value: Uint8Array }[] = [];
	const updatesCount = await db.readAllUpdates(docName, (partial) => {
		// Buffer를 Uint8Array로 변환하여 저장
		updates.push(
			...partial.map((p) => ({
				value: new Uint8Array(p.value),
			})),
		);
	});
	console.log(`[getYDocFromDb] readAllUpdates done. total = ${updatesCount}`);

	// 2) 새로운 Doc 생성
	const ydoc = new Y.Doc();

	// 3) 메모리에 읽어온 업데이트들을 한 번에 apply
	//    - doc.transact는 동기적으로 처리되므로, 여기서 DB 접속은 하지 않음.
	ydoc.transact(() => {
		for (let i = 0; i < updates.length; i++) {
			const row = updates[i];
			// row.value는 Buffer이므로, 바로 new Uint8Array(row.value)로 변환 가능
			const updateArr = row.value;
			Y.applyUpdate(ydoc, updateArr);
		}
	});
	console.log('[getYDocFromDb] all updates applied to ydoc');

	// 4) flushSize 체크 → 너무 많은 업데이트가 쌓였으면 한 번에 병합
	if (updatesCount > flushSize) {
		console.log('[getYDocFromDb] flushDocument start');
		await flushDocument(db, docName, Y.encodeStateAsUpdate(ydoc), Y.encodeStateVector(ydoc));
		console.log('[getYDocFromDb] flushDocument done');
	}

	// 5) 완성된 ydoc 반환
	return ydoc;
};
