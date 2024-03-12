import requests
import psycopg2
from tqdm import tqdm

def get_access_token():
    base_url = "https://www.bukalapak.com/westeros_auth_proxies"

    headers = {
        "Accept":"application/json, text/plain, */*",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "Host":"www.bukalapak.com",
        "Origin": "https://www.bukalapak.com",
        "Referer": "https://www.bukalapak.com/c?from=nav_header",
        "Sec-Fetch-Dest": "",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "TE": "trailers",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"
    }

    payload = {
        "application_id":1,
        "authenticity_token":""}
    
    response = requests.post(base_url, headers=headers, json=payload)
    return response.json()['access_token']

def get_all_categories(conn):
    access_token = get_access_token()

    base_url = "https://api.bukalapak.com/aggregate?access_token=" + access_token

    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Content-Length": "86",
        "Content-Type": "application/json",
        "Host": "api.bukalapak.com",
        "Origin": "https://www.bukalapak.com",
        "Referer": "https://www.bukalapak.com/",
        "Sec-Fetch-Dest": "",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "TE": "trailers",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"
    }

    payload = {
        "aggregate":{
            "getCategories":{
                "method":"GET",
                "path":"/categories",
                "scope":"public"}}}
    
    response = requests.post(url=base_url, headers=headers, json=payload)

    categories = response.json()['data']['getCategories']
    for category in categories:
        cat_id = category['id']
        cat_name = category['name']
        cat_permalink = category['permalink']

        for sub_category in category['children']:
            sub_cat_id = sub_category['id']
            sub_cat_name = sub_category['name']
            sub_cat_permalink = sub_category['permalink']

            for product in sub_category['children']:
                pro_id = product['id']
                pro_name = product['name']
                pro_permalink = product['permalink']

                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "insert into categories_bukalapak values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (cat_id, cat_name, cat_permalink, sub_cat_id, sub_cat_name, sub_cat_permalink, pro_id, pro_name, pro_permalink)
                        )
                        conn.commit()
                        conn.rollback()

                except Exception as e:
                    print("Error => ", e)
                finally:
                    conn.commit()
                    conn.rollback()

def get_all_products(conn):
    try:
        with conn.cursor() as cur:
            cur.execute(f"select max(latest_offset) from metadata_bukalapak")
            offset = cur.fetchone()[0]
            if offset is None:
                offset = 0
            offset = int(offset)
    except Exception as e:
        offset = 0

    with conn.cursor() as cur:
        cur.execute(f"""
                    select * 
                    from categories_bukalapak 
                    group by cat_id, cat_name, cat_permalink, sub_cat_id, sub_cat_name, sub_cat_permalink, pro_id, pro_name, pro_permalink
                    offset {offset}""")
        results = cur.fetchall()

    for result in results:

        cat_id = result[3]

        print(f"BukaLapak Scrape => {result[4]} - Last Offset => {offset}")

        try:
            with conn.cursor() as cur:
                cur.execute(
                    "insert into metadata_bukalapak(latest_offset) values (%s)",
                    (offset,))
            conn.commit()
            conn.rollback()
        except Exception as e:
            print(f"Error => {e}")

        for page in tqdm(range(1, 100), total=100, desc="Page"):
            access_token = get_access_token()

            base_url = f"https://api.bukalapak.com/multistrategy-products?category_id={cat_id}&sort=bestselling&limit=30&offset=30&facet=true&page={page}&shouldUseSeoMultistrategy=false&isLoggedIn=false&show_search_contexts=true&access_token=" + access_token

            headers = {
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.5",
                "Connection": "keep-alive",
                "Host": "api.bukalapak.com",
                "Origin": "https://www.bukalapak.com",
                "Referer": "https://www.bukalapak.com/",
                "Sec-Fetch-Dest": "",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site",
                "TE": "trailers",
                "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
                "X-Device-Ad-ID":"5c801cdb16600cb459eb32ab3cafe15c"
            }

            response = requests.get(base_url, headers=headers)

            if response.status_code == 200:

                for product in response.json()['data']:
                    prod_id = product['id']
                    prod_name = product['name']
                    prod_description = product['description']
                    prod_url = product['url']
                    store_id = product['store']['id']
                    store_name = product['store']['name']
                    store_url = product['store']['url']
                    store_location = f"{product['store']['address']['city']}, {product['store']['address']['province']}"

                    try:
                        with conn.cursor() as cur:
                            cur.execute(
                                "insert into products_bukalapak values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (cat_id, prod_id, prod_name, prod_description, prod_url, store_id, store_name, store_location, store_url))
                        
                        conn.commit()
                        conn.rollback()
                    except Exception as e:
                        print("Error => ",  {e})
            else:
                break
        
        offset += 1


def main():

    conn = psycopg2.connect(**{
            'database': 'xxxxxxxxxx',
            'user': 'xxxxxxxxxx',
            'password': 'xxxxxxxxxx',
            'host': 'xxxxxxxxxx',
            'port': 'xxxxxxxxxx'
            })

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS categories_bukalapak (
                    cat_id BIGINT NOT NULL ,
                    cat_name VARCHAR NOT NULL,
                    cat_permalink VARCHAR NOT NULL,
                    sub_cat_id BIGINT NOT NULL ,
                    sub_cat_name VARCHAR NOT NULL,
                    sub_cat_permalink VARCHAR NOT NULL,
                    pro_id BIGINT NOT NULL PRIMARY KEY,
                    pro_name VARCHAR NOT NULL,
                    pro_permalink VARCHAR NOT NULL)""")
            
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS products_bukalapak (
                    cat_id BIGINT NOT NULL,
                    prod_id VARCHAR NOT NULL,
                    prod_name VARCHAR NOT NULL,
                    prod_description VARCHAR NOT NULL,
                    prod_url VARCHAR NOT NULL,
                    store_id BIGINT NOT NULL,
                    store_name VARCHAR NOT NULL,
                    store_url VARCHAR NOT NULL,
                    store_location VARCHAR NOT NULL)""")
            
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata_bukalapak (
                    latest_offset VARCHAR NOT NULL
                )""")
            
        conn.commit()
    except Exception as e:
        print("Error => ",  {e})

    get_all_categories(conn)        
    get_all_products(conn)


if __name__ == "__main__":
    main()
