import pg, { PoolConfig, Pool as IPool } from 'pg';
import format from 'pg-format';

const { Pool } = pg;

type Update = {
	id: number;
	docname: string;
	value: Uint8Array;
	version: 'v1';
};

/**
 * PgAdapter: PostgreSQL에 Yjs 업데이트를 저장/로드하는 어댑터
 */
export class PgAdapter {
	private tableName: string;
	private pool: IPool;

	constructor(tableName: string, pool: IPool) {
		this.tableName = tableName;
		this.pool = pool;
	}

	/**
	 * -----------------------------------------------------------------------------------
	 * (1) connect()에서 인덱스 생성 논리 포함
	 * -----------------------------------------------------------------------------------
	 */
	static async connect(
		connectionOptions: PoolConfig,
		{ tableName, useIndex }: { tableName: string; useIndex: boolean },
	) {
		const pool = new Pool(connectionOptions);
		// 연결 테스트
		await pool.query('SELECT 1+1;');

		// 테이블 존재 여부 확인
		const tableExistsRes = await pool.query(
			`
      SELECT EXISTS (
        SELECT FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename  = $1
      );
      `,
			[tableName],
		);
		const tableExists = tableExistsRes.rows[0].exists;

		// 테이블이 없으면 생성
		if (!tableExists) {
			// enum 타입 생성
			await pool.query(`
      DO $$
      BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ypga_version') THEN
          CREATE TYPE ypga_version AS ENUM ('v1', 'v1_sv');
        END IF;
      END
      $$;
      `);

			// yjs-writings 등 사용자 지정 테이블
			await pool.query(
				format(
					`
          CREATE TABLE %I (
              id SERIAL PRIMARY KEY,
              docname TEXT NOT NULL,
              value BYTEA NOT NULL,
              version ypga_version NOT NULL
          );`,
					tableName,
				),
			);
		}

		// docName 인덱스 생성
		if (useIndex) {
			const indexName = `${tableName}_docname_idx`;
			const indexExistsRes = await pool.query(
				`
        SELECT EXISTS (
          SELECT FROM pg_indexes
          WHERE tablename = $1
            AND indexname = $2
        );
        `,
				[tableName, indexName],
			);
			const indexDocNameExists = indexExistsRes.rows[0].exists;

			if (!indexDocNameExists) {
				await pool.query(format(`CREATE INDEX %I ON %I (docname);`, indexName, tableName));
			}

			// (옵션) docname+id 복합 인덱스: 대량 데이터에서 "ORDER BY id"시 더욱 최적화
			const idx2 = `${tableName}_docname_id_idx`;
			const idx2Exists = await pool.query(
				`
			  SELECT EXISTS (
			    SELECT FROM pg_indexes
			    WHERE tablename = $1
			      AND indexname = $2
			  );
			  `,
				[tableName, idx2],
			);
			if (!idx2Exists.rows[0].exists) {
				await pool.query(format('CREATE INDEX %I ON %I (docname, id);', idx2, tableName));
			}
		}

		return new PgAdapter(tableName, pool);
	}

	/**
	 * -----------------------------------------------------------------------------------
	 * (2) findLatestDocumentId(): 가장 최근(큰 id) 가져오기
	 * -----------------------------------------------------------------------------------
	 */
	async findLatestDocumentId(docName: string) {
		const query = format(
			`
      SELECT id FROM %I
      WHERE docname = %L
      AND version = 'v1'
      ORDER BY id DESC
      LIMIT 1;`,
			this.tableName,
			docName,
		);
		const res = await this.pool.query(query);
		return res.rows[0]?.id ?? -1;
	}

	/**
	 * -----------------------------------------------------------------------------------
	 * (3) insertUpdate(): 업데이트 1개 row 추가
	 * -----------------------------------------------------------------------------------
	 */
	async insertUpdate(docName: string, value: Uint8Array) {
		// 1. 커넥션 풀링 최적화
		const client = await this.pool.connect();

		try {
			// 2. 트랜잭션 사용
			await client.query('BEGIN');

			const bufferValue = Buffer.from(value);
			const query = format(
				`INSERT INTO %I (docname, value, version)
				 VALUES (%L, %L, 'v1')
				 RETURNING id;`, // 3. 필요한 컬럼만 반환
				this.tableName,
				docName,
				bufferValue,
			);

			const res = await client.query(query);
			await client.query('COMMIT');

			return { id: res.rows[0].id };
		} catch (error) {
			await client.query('ROLLBACK');
			throw error;
		} finally {
			client.release();
		}
	}

	/**
	 * 내부적으로 state vector(버전='v1_sv') 로우 조회
	 */
	private async _getStateVector(docName: string) {
		const query = format(
			`
      SELECT id, value FROM %I
      WHERE docname = %L
        AND version = 'v1_sv'
      LIMIT 1;`,
			this.tableName,
			docName,
		);
		const res = await this.pool.query(query);
		return res.rows[0];
	}

	/**
	 * -----------------------------------------------------------------------------------
	 * (4) getStateVectorBuffer(): docName의 state vector row 조회
	 * -----------------------------------------------------------------------------------
	 */
	async getStateVectorBuffer(docName: string) {
		const svRow = await this._getStateVector(docName);
		return svRow?.value as Uint8Array | null;
	}

	/**
	 * -----------------------------------------------------------------------------------
	 * (5) putStateVector(): docName의 state vector를 upsert
	 * -----------------------------------------------------------------------------------
	 */
	async putStateVector(docName: string, value: Uint8Array) {
		const bufferValue = Buffer.from(value);
		const sv = await this._getStateVector(docName);

		let query;
		if (sv) {
			// 기존 row update
			query = format(
				`
        UPDATE %I
        SET value = %L
        WHERE id = %L
        RETURNING *;`,
				this.tableName,
				bufferValue,
				sv.id,
			);
		} else {
			// 새로 삽입
			query = format(
				`
        INSERT INTO %I (docname, value, version)
        VALUES (%L, %L, 'v1_sv')
        RETURNING *;`,
				this.tableName,
				docName,
				bufferValue,
			);
		}

		const res = await this.pool.query(query);
		return res.rows[0];
	}

	/**
	 * -----------------------------------------------------------------------------------
	 * (6) clearUpdatesRange(): 특정 범위의 updates 삭제
	 * -----------------------------------------------------------------------------------
	 */
	async clearUpdatesRange(docName: string, from: number, to: number) {
		const query = format(
			`
      DELETE FROM %I
      WHERE docname = %L
        AND version = 'v1'
        AND id >= %L
        AND id < %L;`,
			this.tableName,
			docName,
			from,
			to,
		);
		await this.pool.query(query);
	}

	/**
	 * -----------------------------------------------------------------------------------
	 * (7) readAllUpdates(): docName에 해당하는 모든 'v1' 업데이트를 한번에 가져오기
	 *     기존 readUpdatesAsCursor()의 LIMIT+OFFSET 반복 대신,
	 *     단일 쿼리로 모두 읽어 callback에 넘김 → 대량 데이터시 OFFSET 문제 해결
	 * -----------------------------------------------------------------------------------
	 */
	async readAllUpdates(docName: string, callback: (records: Update[]) => void) {
		const query = format(
			`
      SELECT id, docname, value
      FROM %I
      WHERE docname = %L
        AND version = 'v1'
      ORDER BY id;`,
			this.tableName,
			docName,
		);

		console.log('[query] readAllUpdates', new Date().toISOString());

		const res = await this.pool.query(query);
		console.log('[res] readAllUpdates', new Date().toISOString());

		const rows = res.rows.map((row) => ({
			id: row.id,
			docname: row.docname,
			value: row.value, // Buffer -> Uint8Array로 변환 시점은 callback에서 처리 가능
			version: 'v1' as const,
		}));
		callback(rows);
		return rows.length;
	}

	/**
	 * -----------------------------------------------------------------------------------
	 * (8) deleteDocument(): 하나의 docName 전체 row 삭제
	 * -----------------------------------------------------------------------------------
	 */
	async deleteDocument(docName: string) {
		const query = format(
			`
      DELETE FROM %I
      WHERE docname = %L
      RETURNING *;`,
			this.tableName,
			docName,
		);
		const res = await this.pool.query(query);
		return res.rows;
	}

	/**
	 * -----------------------------------------------------------------------------------
	 * (9) close(): Pool 종료
	 * -----------------------------------------------------------------------------------
	 */
	async close() {
		await this.pool.end();
	}
}
