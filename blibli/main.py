import requests
import psycopg2
from tqdm import tqdm

def get_categories(conn):
    base_url = "https://www.blibli.com/backend/common/categories"

    headers = {
        "Accept":"application/json, text/plain, */*",
        "Accept-Encoding":"gzip,deflate,br",
        "Accept-Language":"en-US,en;q=0.5",
        "Cache-Control":"no-cache",
        "Connection":"keep-alive",
        "Host":"www.blibli.com",
        "Referer":"https://www.blibli.com/",
        "Sec-Fetch-Dest":"",
        "Sec-Fetch-Mode":"cors",
        "Sec-Fetch-Site":"same-origin",
        "User-Agent":"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"
    }

    response = requests.get(base_url, headers=headers)

    for category in response.json()['data']:
        cat_id = category['id']
        cat_code = category['categoryCode']
        cat_name = category['name']
        cat_redirect_url = category['redirectUrl']

        sub_cat_url = f"https://www.blibli.com/backend/common/categories/{cat_id}/children"

        headers = {
            "Accept":"application/json, text/plain, */*",
            "Accept-Encoding":"gzip,deflate,br",
            "Accept-Language":"en-US,en;q=0.5",
            "Cache-Control":"no-cache",
            "Connection":"keep-alive",
            "Host":"www.blibli.com",
            "Referer": "https://www.blibli.com" + cat_redirect_url,
            "Sec-Fetch-Dest":"",
            "Sec-Fetch-Mode":"cors",
            "Sec-Fetch-Site":"same-origin",
            "User-Agent":"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"
        }

        sub_response = requests.get(sub_cat_url, headers=headers)

        for sub_category in sub_response.json()['data']:
            sub_cat_id = sub_category['id']
            sub_cat_code = sub_category['categoryCode']
            sub_cat_name = sub_category['name']
            sub_cat_redirect_url = sub_category['redirectUrl']

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "insert into categories_blibli values (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (cat_id, cat_code, cat_name, sub_cat_id, sub_cat_code, sub_cat_name, cat_redirect_url, sub_cat_redirect_url))
                conn.commit()
                conn.rollback()
            except Exception as e:
                print(f"Error => {e}")

def get_products(conn):
    try:
        with conn.cursor() as cur:
            cur.execute(f"select max(latest_offset) from metadata_blibli")
            offset = cur.fetchone()[0]
            if offset is None:
                offset = 0
            offset = int(offset)
            
    except Exception as e:
        offset = 0

    with conn.cursor() as cur:
        cur.execute(f"""
                    select * 
                    from categories_blibli 
                    group by cat_id , cat_code , cat_name , sub_cat_id , sub_cat_code , sub_cat_name , cat_redirect_url , sub_cat_redirect_url 
                    offset {offset}""")
        results = cur.fetchall()
    conn.commit()
    conn.rollback()
    
    for result in results:
        sub_cat_code = result[4]

        print(f"Blibli Scrape => {result[5]} - Last Offset => {offset}")

        try:
            with conn.cursor() as cur:
                cur.execute(
                    "insert into metadata_blibli (latest_offset) values (%s)",
                    (offset,))
            conn.commit()
            conn.rollback()
        except Exception as e:
            print(f"Error => {e}")

        for i in tqdm(range(0, 51), total=50, desc="Page"):
            base_url = f"https://www.blibli.com/backend/search/products?category={sub_cat_code}&start={i * 40}&channelId=web&isMobileBCA=false&showFacet=false"
            headers = {
                "Accept":"application/json, text/plain, */*",
                "Accept-Encoding":"gzip,deflate,br",
                "Accept-Language":"en-US,en;q=0.5",
                "Cache-Control":"no-cache",
                "Connection":"keep-alive",
                "Host":"www.blibli.com",
                "Sec-Fetch-Dest":"",
                "Sec-Fetch-Mode":"cors",
                "Sec-Fetch-Site":"same-origin",
                "User-Agent":"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"}
            response = requests.get(base_url, headers=headers)

            if response.status_code == 200:
                for product in response.json()['data']['products']:
                    prod_id = product['id']
                    prod_name = product['name']
                    prod_url = product['url']
                    shop_code = product['merchantCode']

                    shop_base_url = f"https://www.blibli.com/backend/search/merchant/{shop_code}?excludeProductList=false&promoTab=false&multiCategory=true&merchantSearch=false&pickupPointLatLong=&showFacet=false&defaultPickupPoint=true"
                    shop = requests.get(shop_base_url, headers=headers).json()['data']['merchant']
                    shop_name = shop['name']
                    shop_description = shop['description']
                    shop_url = shop['url']

                    try:
                        shop_address = shop['pickupPoint']['address']
                    except:
                        shop_address = ""

                    try:
                        with conn.cursor() as cur:
                            cur.execute(
                                "insert into products_blibli values(%s, %s, %s, %s, %s, %s, %s, %s)",
                                (prod_id, prod_name, prod_url, shop_code, shop_name, shop_description, shop_address, shop_url)
                            )
                        conn.commit()
                        conn.rollback()
                    except Exception as e:
                        print(f"Error => {e}")

            else:
                break
        
        offset += 1

def main():

    conn = psycopg2.connect(**{
            'database': 'xxxxxxx',
            'user': 'xxxxxxx',
            'password': 'xxxxxxx',
            'host': 'xxxxxxx',
            'port': 'xxxxxxx'
            })
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS categories_blibli (
                    cat_id VARCHAR NOT NULL,
                    cat_code VARCHAR NOT NULL,
                    cat_name VARCHAR NOT NULL,
                    sub_cat_id VARCHAR NOT NULL PRIMARY KEY,
                    sub_cat_code VARCHAR NOT NULL,
                    sub_cat_name VARCHAR NOT NULL,
                    cat_redirect_url VARCHAR NOT NULL,
                    sub_cat_redirect_url VARCHAR NOT NULL)""")
            
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS products_blibli (
                    prod_id VARCHAR NOT NULL,
                    prod_name VARCHAR NOT NULL,
                    prod_url VARCHAR NOT NULL,
                    shop_code VARCHAR NOT NULL,
                    shop_name VARCHAR NOT NULL,
                    shop_description VARCHAR NOT NULL,
                    shop_address VARCHAR NOT NULL,
                    shop_url VARCHAR NOT NULL)""")
            
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata_blibli (
                    latest_offset VARCHAR NOT NULL
                )""")
            
        conn.commit()
    except Exception as e:
        print("Error => ",  {e})

    get_categories(conn)
    get_products(conn)


if __name__ == "__main__":
    main()
