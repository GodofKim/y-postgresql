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

	// 최초 상태벡터 생성을 비동기로 처리
	if (clock === -1) {
		Promise.resolve()
			.then(async () => {
				const ydoc = new Y.Doc();
				Y.applyUpdate(ydoc, update);
				const sv = Y.encodeStateVector(ydoc);
				await writeStateVector(db, docName, sv, 0);
			})
			.catch((err) => console.error('[storeUpdate] Failed to write initial state vector:', err));
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
	console.log('[getYDocFromDb] start');

	// 1) 새로운 Doc 미리 생성
	const ydoc = new Y.Doc();

	// 2) 스트리밍 방식으로 업데이트 적용
	let batchCount = 0;
	await db.readAllUpdates(docName, (partial) => {
		// 배치 단위로 트랜잭션 처리
		ydoc.transact(
			() => {
				for (const p of partial) {
					Y.applyUpdate(ydoc, new Uint8Array(p.value));
				}
			},
			null,
			true,
		); // 세 번째 파라미터 true: 원격 변경사항으로 처리

		batchCount++;
		if (batchCount % 10 === 0) {
			console.log(`[getYDocFromDb] processed ${batchCount} batches`);
		}
	});

	// 3) flushSize 체크 - 비동기로 처리
	const totalUpdates = batchCount * 1000; // BATCH_SIZE = 1000 기준
	if (totalUpdates > flushSize) {
		console.log('[getYDocFromDb] starting async flush');
		flushDocument(db, docName, Y.encodeStateAsUpdate(ydoc), Y.encodeStateVector(ydoc))
			.then(() => console.log('[getYDocFromDb] async flush complete'))
			.catch((err) => console.error('[getYDocFromDb] flush error:', err));
	}

	console.log('[getYDocFromDb] complete');
	return ydoc;
};
