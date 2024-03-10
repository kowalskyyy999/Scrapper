import requests
import psycopg2
from tqdm import tqdm

def get_all_categories(conn=None):
    url = "https://gql.tokopedia.com/graphql/headerMainData"

    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "content-type": "application/json",
        "Host": "gql.tokopedia.com",
        "Origin": "https://www.tokopedia.com",
        "Referer": "https://www.tokopedia.com/",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Sec-Fetch-Dest": "",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site"}

    payload = {
        "operationName": "headerMainData",
        "query": "query headerMainData($filter: String) {\n dynamicHomeIcon {\n categoryGroup {\n id\n title\n desc\n categoryRows {\n id\n name\n url\n imageUrl\n type\n categoryId\n __typename\n }\n __typename\n }\n __typename\n }\n categoryAllListLite(filter: $filter) {\n categories {\n id\n name\n url\n iconImageUrl\n isCrawlable\n children {\n id\n name\n url\n isCrawlable\n children {\n id\n name\n url\n isCrawlable\n __typename\n }\n __typename\n }\n __typename\n }\n __typename\n }\n}\n"
    }

    response = requests.post(url, json=payload, headers=headers)
    data = response.json()

    category_list = data["data"]["categoryAllListLite"]["categories"]

    for category in category_list:
        
        category_id = category["id"]
        category_name = category["name"]
        category_url = category["url"]

        try:
            with conn.cursor() as cur:
                cur.execute(
                    "insert into category_tokopedia values (%s, %s, %s)",
                    (category_id, category_name, category_url)
                )
            conn.commit()
            conn.rollback()
        except Exception as e:
            print(f"Error => {e}")

        sub_category_list = category["children"]
        
        for sub_category in sub_category_list:
            
            sub_category_id = sub_category["id"]
            sub_category_name = sub_category["name"]
            sub_category_url = sub_category["url"]

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "insert into sub_category_tokopedia values (%s, %s, %s, %s)",
                        (sub_category_id, category_id, sub_category_name, sub_category_url)
                    )
                conn.commit()
                conn.rollback()
            except Exception as e:
                print(f"Error => {e}")

            product_list = sub_category['children']

            for product in product_list:
                product_id = product['id']
                product_name = product['name']
                product_url = product["url"]

                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "insert into product_category_tokopedia values (%s, %s, %s, %s)",
                            (product_id, sub_category_id, product_name, product_url)
                        )
                    conn.commit()
                    conn.rollback()
                except Exception as e:
                    print(f"Error => {e}")

def search_all_product(conn=None):
    try:
        with conn.cursor() as cur:
            cur.execute(f"select max(latest_offset) from metadata_tokopedia")
            offset = cur.fetchone()[0]
            if offset is None:
                offset = 0
            offset = int(offset)

    except Exception as e:
        offset = 0

    with conn.cursor() as cur:
        cur.execute(f"""
                    select * 
                    from product_category_tokopedia 
                    group by  id, sub_cat_id, name, url 
                    offset {offset}""")
        results = cur.fetchall()
    
    base_url = "https://gql.tokopedia.com/graphql/SearchProductQuery"

    for product in results:
        product_url = product[-1]
        product_id = product[0]
        identifier = product_url.split("https://www.tokopedia.com/p/")[-1].replace("/", "_")
        
        headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "content-type": "application/json",
            "Host": "gql.tokopedia.com",
            "Origin": "https://www.tokopedia.com",
            "Referer": f"{product_url}?page=1",
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Sec-Fetch-Dest": "",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "TE": "trailers",
            "Tkpd-UserId": "0",
            "iris_session_id": "d3d3LnRva29wZWRpYS5jb20=.8f872130a003c4c00b70980797d95d57.1695714400540"}
        
        payload = {
            "operationName":"SearchProductQuery",
            "variables":{
                "params":f"page=1&ob=&identifier={identifier}&sc={product_id}&user_id=0&rows=60&start=121&source=directory&device=desktop&page=1&related=true&st=product&safe_search=false",
                "adParams":f"page=1&page=1&dep_id={product_id}&ob=&ep=product&item=15&src=directory&device=desktop&user_id=0&minimum_item=15&start=121&no_autofill_range=5-14"},
            "query":"query SearchProductQuery($params: String, $adParams: String) {\n  CategoryProducts: searchProduct(params: $params) {\n    count\n    data: products {\n      id\n      url\n      imageUrl: image_url\n      imageUrlLarge: image_url_700\n      catId: category_id\n      gaKey: ga_key\n      countReview: count_review\n      discountPercentage: discount_percentage\n      preorder: is_preorder\n      name\n      price\n      priceInt: price_int\n      original_price\n      rating\n      wishlist\n      labels {\n        title\n        color\n        __typename\n      }\n      badges {\n        imageUrl: image_url\n        show\n        __typename\n      }\n      shop {\n        id\n        url\n        name\n        goldmerchant: is_power_badge\n        official: is_official\n        reputation\n        clover\n        location\n        __typename\n      }\n      labelGroups: label_groups {\n        position\n        title\n        type\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  displayAdsV3(displayParams: $adParams) {\n    data {\n      id\n      ad_ref_key\n      redirect\n      sticker_id\n      sticker_image\n      productWishListUrl: product_wishlist_url\n      clickTrackUrl: product_click_url\n      shop_click_url\n      product {\n        id\n        name\n        wishlist\n        image {\n          imageUrl: s_ecs\n          trackerImageUrl: s_url\n          __typename\n        }\n        url: uri\n        relative_uri\n        price: price_format\n        campaign {\n          original_price\n          discountPercentage: discount_percentage\n          __typename\n        }\n        wholeSalePrice: wholesale_price {\n          quantityMin: quantity_min_format\n          quantityMax: quantity_max_format\n          price: price_format\n          __typename\n        }\n        count_talk_format\n        countReview: count_review_format\n        category {\n          id\n          __typename\n        }\n        preorder: product_preorder\n        product_wholesale\n        free_return\n        isNewProduct: product_new_label\n        cashback: product_cashback_rate\n        rating: product_rating\n        top_label\n        bottomLabel: bottom_label\n        __typename\n      }\n      shop {\n        image_product {\n          image_url\n          __typename\n        }\n        id\n        name\n        domain\n        location\n        city\n        tagline\n        goldmerchant: gold_shop\n        gold_shop_badge\n        official: shop_is_official\n        lucky_shop\n        uri\n        owner_id\n        is_owner\n        badges {\n          title\n          image_url\n          show\n          __typename\n        }\n        __typename\n      }\n      applinks\n      __typename\n    }\n    template {\n      isAd: is_ad\n      __typename\n    }\n    __typename\n  }\n}\n"
            }
        
        response = requests.post(base_url, json=payload, headers=headers)

        print(f"Scrape => {product[2]} {response} - Last Offset => {offset}")

        try:
            with conn.cursor() as cur:
                cur.execute(
                    "insert into metadata_tokopedia(latest_offset) values (%s)",
                    (offset,))
            conn.commit()
            conn.rollback()
        except Exception as e:
            print(f"Error => {e}")

        if response.status_code == 200:
            data = response.json()

            for page in tqdm(range(1, 110 + 1), total=110, desc="Pages"):
                payload = {
                    "operationName":"SearchProductQuery",
                    "variables":{
                        "params":f"page={page}&ob=&identifier={identifier}&sc={product_id}&user_id=0&rows=60&start=121&source=directory&device=desktop&page={page}&related=true&st=product&safe_search=false",
                        "adParams":f"page={page}&page={page}&dep_id={product_id}&ob=&ep=product&item=15&src=directory&device=desktop&user_id=0&minimum_item=15&start=121&no_autofill_range=5-14"},
                    "query":"query SearchProductQuery($params: String, $adParams: String) {\n  CategoryProducts: searchProduct(params: $params) {\n    count\n    data: products {\n      id\n      url\n      imageUrl: image_url\n      imageUrlLarge: image_url_700\n      catId: category_id\n      gaKey: ga_key\n      countReview: count_review\n      discountPercentage: discount_percentage\n      preorder: is_preorder\n      name\n      price\n      priceInt: price_int\n      original_price\n      rating\n      wishlist\n      labels {\n        title\n        color\n        __typename\n      }\n      badges {\n        imageUrl: image_url\n        show\n        __typename\n      }\n      shop {\n        id\n        url\n        name\n        goldmerchant: is_power_badge\n        official: is_official\n        reputation\n        clover\n        location\n        __typename\n      }\n      labelGroups: label_groups {\n        position\n        title\n        type\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  displayAdsV3(displayParams: $adParams) {\n    data {\n      id\n      ad_ref_key\n      redirect\n      sticker_id\n      sticker_image\n      productWishListUrl: product_wishlist_url\n      clickTrackUrl: product_click_url\n      shop_click_url\n      product {\n        id\n        name\n        wishlist\n        image {\n          imageUrl: s_ecs\n          trackerImageUrl: s_url\n          __typename\n        }\n        url: uri\n        relative_uri\n        price: price_format\n        campaign {\n          original_price\n          discountPercentage: discount_percentage\n          __typename\n        }\n        wholeSalePrice: wholesale_price {\n          quantityMin: quantity_min_format\n          quantityMax: quantity_max_format\n          price: price_format\n          __typename\n        }\n        count_talk_format\n        countReview: count_review_format\n        category {\n          id\n          __typename\n        }\n        preorder: product_preorder\n        product_wholesale\n        free_return\n        isNewProduct: product_new_label\n        cashback: product_cashback_rate\n        rating: product_rating\n        top_label\n        bottomLabel: bottom_label\n        __typename\n      }\n      shop {\n        image_product {\n          image_url\n          __typename\n        }\n        id\n        name\n        domain\n        location\n        city\n        tagline\n        goldmerchant: gold_shop\n        gold_shop_badge\n        official: shop_is_official\n        lucky_shop\n        uri\n        owner_id\n        is_owner\n        badges {\n          title\n          image_url\n          show\n          __typename\n        }\n        __typename\n      }\n      applinks\n      __typename\n    }\n    template {\n      isAd: is_ad\n      __typename\n    }\n    __typename\n  }\n}\n"
                    }

                response = requests.post(base_url, json=payload, headers=headers)
                if response.status_code == 200:
                    datas = response.json()
                    for data in datas['data']['CategoryProducts']['data']:
                        cat_id = data['catId']
                        url_key = data['gaKey']
                        id = data['id']
                        name = data['name']
                        url = data['url']
                        shop_id = data['shop']['id']
                        shop_location = data['shop']['location']
                        shop_name = data['shop']['name']
                        shop_url = data['shop']['url']

                        try:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "insert into products_tokopedia values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                    (id, cat_id, name, url_key, url, shop_id, shop_name, shop_location, shop_url)
                                )
                            conn.commit()
                            conn.rollback()
                        except Exception as e:
                            print("Error => ", e)
                else:
                    break
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
                CREATE TABLE IF NOT EXISTS category_tokopedia (
                    id INT NOT NULL,
                    name VARCHAR NOT NULL,
                    url VARCHAR NOT NULL)
            """)

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sub_category_tokopedia (
                    id INT NOT NULL ,
                    cat_id INT NOT NULL, 
                    name VARCHAR NOT NULL,
                    url VARCHAR NOT NULL)""") 

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS product_category_tokopedia (
                    id INT NOT NULL,
                    sub_cat_id INT NOT NULL,
                    name VARCHAR NOT NULL,
                    url VARCHAR NOT NULL)""")
            
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS products_tokopedia (
                    id BIGINT NOT NULL,
                    cat_id INT,
                    name VARCHAR,
                    url_key VARCHAR,
                    url_full VARCHAR,
                    shop_id BIGINT,
                    shop_name VARCHAR,
                    shop_location VARCHAR,
                    shop_url VARCHAR)""")
            
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata_tokopedia (
                    latest_offset VARCHAR
                )""")
            
        conn.commit()
    except Exception as e:
        print("Error => ",  {e})

    get_all_categories(conn)
    search_all_product(conn)

if __name__ == "__main__":
    main()
