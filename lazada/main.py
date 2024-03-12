import requests
import psycopg2
import re
from tqdm import tqdm

def get_token():
    base_url = "https://member.lazada.co.id/user/api/getCsrfToken"

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding":"gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "Host": "member.lazada.co.id",
        "Origin": "https://www.lazada.co.id",
        "Referer": "https://www.lazada.co.id/",
        "Sec-Fetch-Dest": "",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "TE":"trailers",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"
    }

    response = requests.get(base_url, headers=headers)
    return response.json()['module']['token']

def get_categories(conn=None):

    token = get_token()

    base_url = 'https://acs-m.lazada.co.id/h5/mtop.lazada.guided.shopping.categories.categorieslpcommon/1.0/?jsv=2.7.2&appKey=24677475&t=1710092479789&sign=556481c5a9f91335b9fb809e369d2013&api=mtop.lazada.guided.shopping.categories.categoriesLpCommon&v=1.0&type=originaljson&isSec=1&AntiCreep=true&timeout=200000000&dataType=json&sessionOption=AutoLoginOnly&x-i18n-language=id&x-i18n-regionID=ID&data={"regionId":"ID","language":"id","platform":"pc","isbackup":"true","backupParams":"language,regionID,platform,pageNo","moduleName":"categoriesLpMultiFloor"}'

    headers = {
        "Accept": "application/json",
        "Accept-Encoding":"gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded",
        "Host": "acs-m.lazada.co.id",
        "Origin": "https://www.lazada.co.id",
        "Referer": "https://www.lazada.co.id/",
        "Sec-Fetch-Dest": "",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "TE":"trailers",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"
    }

    cookies =  {
		"_fbp": "fb.2.1710087739444.630259131",
		"_ga": "GA1.3.223208703.1710087739",
		"_ga_44FMGEPY40": "GS1.3.1710087739.1.1.1710088504.60.0.0",
		"_gcl_au": "1.1.1722664821.1710087371",
		"_gid": "GA1.3.1132232793.1710087739",
		"_m_h5_tk": "9ea44ae169e63453cd5bb5ead674e5df_1710102199787",
		"_m_h5_tk_enc": "5172eac0fe7c4cf204257156fb3fb571",
		"_tb_token_": token,
		"_uetsid": "5db29e80defa11ee91d32338297c1880",
		"_uetvid": "5db2dbd0defa11ee970b710146220431",
		"AMCV_126E248D54200F960A4C98C6@AdobeOrg": "-1124106680|MCIDTS|19793|MCMID|26294841602240664873921376683010225226|MCOPTOUT-1710095704s|NONE|vVersion|5.2.0",
		"AMCVS_126E248D54200F960A4C98C6@AdobeOrg": "1",
		"cna": "W0BsHhh4szsCAWdpRGImkDJf",
		"epssw": "1*iWVs11ifNf3cGADM7zFQKJtIf4zBu8MsvwJR8lP59WXiIUeFsQlSxdMPIA5L5QmlqGxMdtJbQpv2jTW_JbwAjGjGs9zpKtJXYUBldt5WH165NOhBFCLrpoI9NrdSEo_C1ZUvIbXSX13ntQdrdE3Y4DZn_YURp5ZZdTHBe4QRyaLWsUQRFtrnxDmngpnNTLfb15fA_0cIQ9x0YF17y4HpeJm1YMm82zQJxkc.",
		"hng": "ID|id-ID|IDR|360",
		"hng.sig": "dJwrVwSueShOlZz95EBCvlH9FLAVtzGZ3msUnc25HIQ",
		"isg": "BHBwlR5Y4Z7WKL2Ue-t7cevjQjfCuVQD5o7x_mrAbEpaJRTPEsurkp1dfbVFrgzb",
		"lwrid": "AQGOB8m0V2zX8tvhZOOVe1x29I/c",
		"lzd_cid": "af2bd161-9b48-4699-e337-780a0131f3c4",
		"lzd_sid": "17d0abf01e0199686e834ab6433bb0d3",
		"t_fv": "1709400426236",
		"t_sid": "O4L5tsYO6orBwrFcMbhN3aNRTf7S0N6o",
		"t_uid": "9VYeFOkV6wB8Z7FVjDTjURBCVuRCxL7H",
		"tfstk": "eAYkA9gOtlVIi-GHCLQSoT4EDmo927_CIpUdpwBE0tWbe8UJTXbHZpYKNBkWKnvJQTzJJadendJGegoSF9VWod4LyYiWTybO8AHtWVd7Nw_EB6cTk6dS77qZJV39NQ7C8AHtWbtlzQsbMoHfSmLoZGBg6NPNrqPjeoAlnyzMh_jVRV608y8cZRBwao4U8EflVg8Y0lWkDy1q9EqQDg5fiOBcMbEJpbst7jc0f0sPG_HtijqQAg5fiOhmilNA4s1-B",
		"utm_channel": "NA",
		"xlly_s": "1"
	}

    
    response = requests.get(base_url, headers=headers, cookies=cookies)
    try:

        for category in response.json()['data']['resultValue']['categoriesLpMultiFloor']['data']:
            cat_name = category['categoryName']
            cat_id = category['id']
            for sub_category in category['level2TabList']:
                sub_cat_id = sub_category['categoryId']
                sub_cat_name = sub_category['categoryName']
                sub_cat_url = sub_category['categoryUrl'][2:]

                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "insert into categories_lazada values (%s, %s, %s, %s, %s)",
                            (cat_id, cat_name, sub_cat_id, sub_cat_name, sub_cat_url)
                        )
                    conn.commit()
                    conn.rollback()
                except Exception as e:
                    print(f"Error => {e}")
    except Exception as e:
        print(f"Error => {e}")

def get_products(conn=None):

    try:
        with conn.cursor() as cur:
            cur.execute(f"select max(latest_offset) from metadata_lazada")
            offset = cur.fetchone()[0]
            if offset is None:
                offset = 0
            offset = int(offset)
    except Exception as e:
        offset = 0

    with conn.cursor() as cur:
        cur.execute(f"""
                    select *
                    from categories_lazada
                    group by cat_id,cat_name,sub_cat_id,sub_cat_name,sub_cat_url
                    offset {offset}""")
        results = cur.fetchall()
    conn.commit()
    conn.rollback()

    for result in results:
        token = get_token()
        sub_cat_name = result[3]

        print(f"Lazada Scrape => {sub_cat_name} - Last Offset => {offset}")

        try:
            with conn.cursor() as cur:
                cur.execute(
                    "insert into metadata_lazada (latest_offset) values (%s)",
                    (offset,))
            conn.commit()
            conn.rollback()
        except Exception as e:
            print(f"Error => {e}")

        for i in tqdm(range(1, 51), total=50, desc="Page"):
            
            base_url = f"https://{result[-1]}/?ajax=true&isFirstRequest=true&page={i}"
            print(base_url)
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Connection": "keep-alive",
                "Host": "www.lazada.co.id",
                "Referer": "https://www.lazada.co.id/",
                "Sec-Fetch-Dest": "",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site",
                "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
                "X-CSRF-TOKEN": token
            }

            cookies = {
	        	"__itrace_wid": "165f4897-e2eb-4008-9125-12755ba7bef0",
	        	"__wpkreporterwid_": "ff9c19e2-ef1a-49e8-b61c-ac546df94b24",
	        	"_bl_uid": "h3lw4tXUcn9gbtmUwc7LqXt6LLw9",
	        	"_m_h5_tk": "fc685202b026ee850a74469764406272_1710089428334",
	        	"_m_h5_tk_enc": "7865d21ec508d05c966e460e8cc77437",
	        	"_tb_token_": token,
	        	"cna": "W0BsHhh4szsCAWdpRGImkDJf",
	        	"epssw": "1*iAds11gcc5fdgdDMuWsGt_9Std2GIdkGiSFrPb1lnRNkBfZgSsfhyjNYClwiI6mj9xRpyrkMfXXkZGt6jhAWH9UT6L154Zuq6LsGs_uF_T_Vseq76TOOFG-wNOhBFhftbAUAZbSoymqWsegWPmkTAwrW9RAPxjDYwDZnmD2R3P-hdT3RyTB4xD4WsJ9-Pa6Bev9-zFxEtJRo15f69JCL7a0kRNu3et2Jk4EnV3TRy96-",
	        	"hng": "ID|id-ID|IDR|360",
	        	"hng.sig": "dJwrVwSueShOlZz95EBCvlH9FLAVtzGZ3msUnc25HIQ",
	        	"isg": "BNvb7VkROo8dZEb13BIwwEQqaTZFsO-yedfqn80Yt1rxrPuOVYB_AvlqRpQil0eq",
	        	"lwrid": "AQGOB8m0V2zX8tvhZOOVe1x29I/c",
	        	"lzd_cid": "af2bd161-9b48-4699-e337-780a0131f3c4",
	        	"lzd_sid": "17d0abf01e0199686e834ab6433bb0d3",
	        	"t_fv": "1709400426236",
	        	"t_sid": "dNiZ2d3stuwdB8wXU4CUqjErR9zXNKiz",
	        	"t_uid": "9VYeFOkV6wB8Z7FVjDTjURBCVuRCxL7H",
	        	"tfstk": "eMfezZMCmxhsaGDakdAy3MW9ScvpMBqbq_tWrabkRHxnOXilQGIrPyE8pO-Pjh_CNBAWEgSAu6GWAB4zaGS2PpYkr1oMuh8ntQbH47jOYL17JL1azMs1dU1Pyb8lrgUpVyU1JwdJZoZfaS_dJyqV6vFfmijAfQqbc5NfJwdJZ966e_snmQNZKoJQmZKG16P0shgwBhlogGLeInNvbbc5_eof0w4iZbfw8pIyT0LMssCRL0DkKbTwcPzN2mfrGsE7hED-edAv7naBRYHJKFLwcPzZeYpMVF-bRe1..",
	        	"utm_channel": "NA",
	        	"xlly_s": "1"
	        }

            response = requests.get(base_url, headers=headers, cookies=cookies)

            if response.status_code == 200:
                for product in response.json()['mods']['listItems']:
                    prod_name = product['name']
                    prod_id = product['itemId']
                    loct = product['location']
                    shop_name = product['sellerName']
                    shop_id = product['sellerId']

                    prod_name_mod = prod_name.replace('"', "").replace(".", "")
                    prod_url = f"https://www.lazada.co.id/products/{prod_name_mod.replace(' ', '-').lower()}-i{prod_id}.html?"         
                    response = requests.get(prod_url)
                    if response.status_code == 200:
                        result_regex = re.search(r'<article>(.*?)</article>', response.text)
                        try:
                            prod_description = result_regex.groups()[0].replace("<p><span>", "").replace("</span></p><p><span>", " ").replace("</span></p>", "")
                        except:
                            prod_description = ""

                        shop_url = f"https://www.lazada.co.id/shop/{shop_name.replace(' ', '-').lower()}?"
                    
                        try:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "insert into products_lazada values (%s, %s, %s, %s, %s, %s, %s)",
                                    (prod_id, prod_name, shop_id, shop_name, prod_description, loct, shop_url)
                                )
                            conn.commit()
                            conn.rollback()
                        except Exception as e:
                            print(f"Error => {e}")

                    else:
                        break
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
                CREATE TABLE IF NOT EXISTS categories_lazada (
                    cat_id VARCHAR NOT NULL,
                    cat_name VARCHAR NOT NULL,
                    sub_cat_id VARCHAR NOT NULL,
                    sub_cat_name VARCHAR NOT NULL,
                    sub_cat_url VARCHAR NOT NULL)""")
            
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS products_lazada (
                    product_id VARCHAR,
                    product_name VARCHAR,
                    shop_id VARCHAR,
                    shop_name VARCHAR,
                    product_description VARCHAR,
                    location VARCHAR,
                    shop_url VARCHAR)""")
                        
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata_lazada (
                    latest_offset VARCHAR NOT NULL
                )""")
            
        conn.commit()
    except Exception as e:
        print("Error => ",  {e})

    get_categories(conn)
    get_products(conn)


if __name__ == "__main__":
    main()
