# article_selection.py (simple)
import mysql.connector
import json

# 테이블 없으면 만들기
def _ensure_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS article_links (
            id BIGINT PRIMARY KEY,
            crawling_time DATETIME NOT NULL,
            category VARCHAR(100) NOT NULL,
            article_order INT NOT NULL,
            url TEXT NOT NULL,
            dup_count INT DEFAULT 1,
            is_used TINYINT(1) DEFAULT 0
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS article_contents (
            id BIGINT PRIMARY KEY,
            url TEXT NOT NULL,
            title TEXT,
            tags JSON,
            publish_time VARCHAR(50),
            author VARCHAR(100),
            sub_col JSON,
            content_col JSON
        );
    """)
    conn.commit()
    cur.close()

def init_schema(conn):
    # DAG 첫 태스크: 테이블 없으면 생성, 있으면 그대로 사용
    _ensure_tables(conn)

# --- 오늘 크롤 결과 업서트 ---
def upsert_today_links(conn, links):
    """
    오늘 긁은 링크들을 DB에 반영.
    - 이미 있는 URL  : dup_count 증가 + 최신 메타 갱신
    - 처음 보는 URL  : 신규 INSERT (dup_count = 1)
    - 'id' 없는 행은 스킵(로그만 출력)
    """
    cur = conn.cursor()

    rows = []
    skipped = 0
    for row in links or []:
        # id를 크롤러가 넣어준다고 가정 > 아니야!!! 이제 있어!!!!
        if not all(k in row for k in ("id", "crawling_time", "category", "article_order", "url")):
            skipped += 1
            continue
        rows.append((int(row["id"]), row["crawling_time"], row["category"], row["article_order"], row["url"]))

# 왜인지 안되는 중(content 삽입하려고 넣은 코드)
def upsert_article_contents(conn, contents):
    """
    article_contents 테이블 삽입
    - id, url 기준 중복 시 UPDATE
    - 없으면 INSERT
    - contents: [{id, url, title, tags, publish_time, author, sub_col, content_col}]
    """
    cur = conn.cursor()
    rows = []
    for row in contents or []:
        # id, url 필수
        if 'id' not in row or 'url' not in row:
            continue

        # json.dumps: python 객체를 JSON 문자열로 바꿔줌
        rows.append((
            int(row.get("id", 0)),
            row.get("url", ""),
            row.get("title", ""),
            json.dumps(row.get("tags", []), ensure_ascii=False),
            row.get("publish_time", ""),
            row.get("author", ""),
            json.dumps(row.get("sub_col", []), ensure_ascii=False),
            json.dumps(row.get("content_col", []), ensure_ascii=False)
            # row.get("sub_col", []): 기사 본문 리스트 가져오는데, 없으면 빈 리스트 가져옴
        ))

    if rows:
        # 여기도 cotent 크롤링 코드 일부
        cur.executemany("""
            INSERT INTO article_contents (id, url, title, tags, publish_time, author, sub_col, content_col)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                title=VALUES(title),
                tags=VALUES(tags),
                publish_time=VALUES(publish_time),
                author=VALUES(author),
                sub_col=VALUES(sub_col),
                content_col=VALUES(content_col)
        """, rows)
        conn.commit()

        cur.executemany("""
            INSERT INTO today_article (id, crawling_time, category, article_order, url)
            VALUES (%s, %s, %s, %s, %s)
        """, rows)
        conn.commit()

        # 기존 되는 코드!
        # 1. url이 같은 기사(중복기사) 데이터 처리
        cur.execute("""UPDATE article_links AS a
            -- today_article 테이블에서 집계 데이터를 가져오기 위해 JOIN
            JOIN (
            SELECT
                url,
                COUNT(*)            AS cnt,        -- 중복 개수
                COALESCE(SUM(article_order), 0) AS sum_order
            FROM today_article
            GROUP BY url
            ) AS t ON a.url = t.url
            SET
            a.dup_count     = a.dup_count + t.cnt,
            a.article_order = a.article_order + t.sum_order
            """)    
        conn.commit()

        # 2. 카테고리 합치기 처리 (Python에서 처리)
        # today_article에서 URL별로 모든 카테고리 수집
        cur.execute("""
            SELECT url, GROUP_CONCAT(DISTINCT category ORDER BY category) as all_categories
            FROM today_article
            GROUP BY url
        """)
        url_categories = cur.fetchall()
        
        # 각 URL에 대해 기존 카테고리와 새로운 카테고리 합치기
        for url, new_categories in url_categories:
            if new_categories:
                # 기존 카테고리 가져오기
                cur.execute("SELECT category FROM article_links WHERE url = %s", (url,))
                result = cur.fetchone()
                if result:
                    existing_category = result[0] or ""
                    
                    # 기존 카테고리와 새로운 카테고리 합치기
                    existing_list = [cat.strip() for cat in existing_category.split(',') if cat.strip()]
                    new_list = [cat.strip() for cat in new_categories.split(',') if cat.strip()]
                    
                    # 중복 제거하고 정렬
                    combined_categories = sorted(list(set(existing_list + new_list)))
                    final_category = ','.join(combined_categories)
                    
                    # 업데이트
                    cur.execute("UPDATE article_links SET category = %s WHERE url = %s", (final_category, url))
        
        conn.commit()


        # 3. 신규 기사 데이터 삽입
        cur.execute("""INSERT INTO article_links (id, crawling_time, category, article_order, url)
                    SELECT 
                        t.id,
                        t.crawling_time,
                        t.category,
                        t.article_order,
                        t.url
                    FROM today_article t
                    LEFT JOIN article_links a ON t.url = a.url
                    WHERE a.url IS NULL
                    """)
        conn.commit()

        # 오늘 테이블 비우기
        cur.execute("TRUNCATE TABLE today_article;")
        conn.commit()

        if skipped:
            print(f"[upsert_today_links] skipped rows (missing id or fields): {skipped}")

    cur.close()

# Top5 뽑고 is_used=1 마킹 ---
def select_and_mark_top5(conn):
    cur = conn.cursor()

    # # 기존 마킹 초기화(NO 누적)
    # cur.execute("UPDATE article_links SET is_used = 0 WHERE is_used <> 0;")
    # conn.commit()

    # 아직 사용되지 않은 기사 중에서 Top5 가져오기
    cur.execute("""
        SELECT id, crawling_time, category, url, dup_count
        FROM article_links
        WHERE is_used = 0
        ORDER BY dup_count DESC, article_order DESC, crawling_time DESC
        LIMIT 5;
    """)
    top5 = cur.fetchall()

    # 선택된 5개 마킹
    if top5:
        ids = [row[0] for row in top5]
        for _id in ids:
            cur.execute("UPDATE article_links SET is_used = 1 WHERE id = %s;", (_id,))
        conn.commit()

    cur.close()
    return top5

