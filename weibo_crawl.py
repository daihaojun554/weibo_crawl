import os
from typing import Tuple
import json
import logging
import random
from time import sleep
import csv

import requests


# 配置日志记录器
os.makedirs('./logs', exist_ok=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    '%(asctime)s - %(pathname)s - %(funcName)s - %(module)s - %(levelname)s - %(message)s')

ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)
th = logging.FileHandler('./logs/weibo_crawl.log', encoding='utf-8')
th.setFormatter(formatter)
logger.addHandler(th)


class WeiBoCrawl:
    def __init__(self):
        os.makedirs('./weibo/', exist_ok=True)
        self.file_path = './weibo/weibo_user_info.csv'
        # 将这个信息存入csv文件
        self.row_headers = ['id', '昵称', '头像', '是否认证', '认证原因', '个人简介',
                            '位置', '粉丝数量', '关注数量', '微博数量', 'vvip', 'svip', '性别']
        # 检查文件是否存在
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w', encoding='utf-8-sig', newline='') as f:
                f.write(','.join(self.row_headers)+"\n")

        self.headers = {
            "authority": "www.weibo.com",
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9",
            "cache-control": "no-cache",
            "client-version": "v2.44.38",
            "pragma": "no-cache",
            "referer": "https://www.weibo.com/u/1699432410",
            "sec-ch-ua": "\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "server-version": "v2023.12.11.1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
            "x-xsrf-token": "bAyOoVMf5XeNeBEC-ANwfLb7"
        }
        self.script_path = os.path.dirname(os.path.abspath(__file__))
        self.config_path = os.path.join(self.script_path, 'config.json')
        self.init_config()

    def init_config(self) -> None:
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        self.user_id_list = config['user_id_list']
        logger.info("读取到的 user_id_list: {}".format(self.user_id_list))
        if self.user_id_list.endswith('txt'):
            user_id_list_file = os.path.join(
                self.script_path, self.user_id_list)  # 添加文件路径
            with open(user_id_list_file, 'r', encoding='utf-8') as f:
                self.user_id_list = f.read().splitlines()
                logger.info("当前 user_id_list.txt 文件里面的值： {}".format(
                    self.user_id_list))
        else:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.user_id_list = config['user_id_list']
                logger.info("读取到的 user_id_list: {}".format(self.user_id_list))
        try:
            self.cookies = {}
            self.cookie = config['cookies']
            for cook in self.cookie.split(";"):
                cook = cook.split("=")
                self.cookies[cook[0]] = cook[1]
            if self.cookies:
                logger.info("读取到的 cookies: {}".format(self.cookies))
            else:
                raise Exception('请先配置 cookie')
        except Exception as e:
            logger.error("读取配置文件出错： {}，请配置cookie信息".format(e))
            raise e

    def check_user_info_exist(self, user_id: int) -> bool:
        with open(self.file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            for row in reader:
                if row and row[0] == user_id:
                    logger.info(f"row[0]===>{row[0]},user_id====>{user_id}")
                    return True
        return False

    def get_user_info(self) -> Tuple[int, requests.Response]:
        logger.info("所有的userid====>{}".format(self.user_id_list))
        base_url = "https://www.weibo.com/ajax/profile/info?uid={}"
        # 如果weibo_iser_info.csv里面已经有了信息，就不需要发起请求了
        for user_id in self.user_id_list:
            url = base_url.format(user_id)
            if self.check_user_info_exist(user_id):
                logger.info("{} 已经存在用户信息，跳过".format(user_id))
                continue
            logger.info("正在爬取用户 {} 的信息".format(user_id))
            # 这里可以添加对 url 的请求，例如使用 requests 库进行请求等操作。
            self.headers.update(
                {"referer": f"https://www.weibo.com/u/{user_id}"})
            yield (user_id, requests.get(url=url, headers=self.headers, cookies=self.cookies))
            t = random.uniform(1, 3)
            sleep(t)
            logger.info("{} 请求成功,休眠{}秒~~~~~".format(user_id, t))

    def parse_user_info(self) -> None:
        user_infos = self.get_user_info()
        for userid, user_info in user_infos:
            logger.info("正在获取用户信息，{},{}".format(userid, user_info))
            if user_info.status_code == 200:
                json_data = user_info.json()
                ok = json_data['ok']
                if ok == 1:
                    user_info = json_data['data']['user']
                    # 获取用户信息，例如昵称、头像、性别等。
                    nickname = user_info.get('screen_name', '')
                    avatar = user_info.get('profile_image_url', '')
                    #
                    verified = str(user_info.get('verified', ''))
                    #
                    verified_reason = user_info.get("verified_reason", '')
                    description = user_info.get("description", '')
                    # 位置
                    loaction = user_info.get("location", '')
                    # 粉丝数量
                    followers_count_str = str(user_info.get(
                        "followers_count_str", ''))
                    # 关注别人的数量
                    friends_count = str(user_info.get("friends_count", ''))
                    # 全部微博的数量
                    statuses_count = str(user_info.get("statuses_count", ''))
                    vvip = str(user_info.get("vvip", ''))
                    svip = str(user_info.get('svip', ''))
                    gender = str(user_info.get('gender'))
                    logger.info("获取到当前用户的信息，{},{},{},{},{},{},{},{},{},{},{},{},{}".format(userid, nickname, avatar, verified, verified_reason,
                                description, loaction, followers_count_str, friends_count, statuses_count, vvip, svip, gender))
                    with open(self.file_path, 'a', encoding='utf-8-sig', newline='') as f:
                        csv.writer(f).writerow([userid, nickname, avatar, verified, verified_reason, description,
                                                loaction, followers_count_str, friends_count, statuses_count, vvip, svip, gender])
                else:
                    logger.error("获取用户信息失败，json['data']===>{}".format(ok))
            else:
                logger.error("请求微博接口失败")

    def crawl_one_up_blogs(self, mid, crawl_page=200):
        #  https://www.weibo.com/ajax/statuses/mymblog?uid=1885454921&page=1&feature=0
        for page in range(1, crawl_page+1):
            t = random.uniform(5, 10)
            logger.info('爬取用户id:{} 的第{}页微博,休眠中{}'.format(mid, page, t))
            sleep(t)
            base_url = f'https://www.weibo.com/ajax/statuses/mymblog?uid={mid}&page={page}&feature=0'
            resp = requests.get(url=base_url, headers=self.headers,
                                cookies=self.cookies).json()
            if resp['ok'] == 1:
                if not os.path.exists(os.path.join(self.script_path, 'weibo', str(mid), 'article.csv')):
                    with open(os.path.join(self.script_path, 'weibo', str(mid), 'article.csv'), 'a', encoding='utf-8-sig', newline='') as f:
                        csv.writer(f).writerow(
                            ['id', 'created', 'text', 'comments_count', 'reposts_count', 'attitudes_count'])
                for item in resp['data']['list']:
                    long_text_id = item['mblogid']
                    is_long_text = item['isLongText']
                    already_exists = False
                    with open(os.path.join(self.script_path, 'weibo', str(mid), 'article.csv'), 'r', encoding='utf-8-sig') as f:
                        lines = f.readlines()
                        for line in lines:
                            if str(item['id']) == str(line.split(',')[0]):
                                logger.info("当前博文已经存在，跳过")
                                already_exists = True
                    if already_exists:
                        continue
                    if is_long_text:
                        logger.info("这是长的博文，{}".format(long_text_id))
                        sleep(random.uniform(3, 5))
                        resp = requests.get(
                            f"https://www.weibo.com/ajax/statuses/longtext?id={long_text_id}", headers=self.headers, cookies=self.cookies).json()
                        if resp['ok'] == 1:
                            item['text_raw'] = resp['data']['longTextContent'] if "longTextContent" in resp['data'] else item['text_raw']
                    yield {
                        "id": item['id'],
                        "created": item['created_at'],
                        'text': item['text_raw'],
                        # 评论内容
                        'comments_count': item['comments_count'],
                        # 转发数量
                        'reposts_count': item['reposts_count'],
                        # 点赞数量
                        'attitudes_count': item['attitudes_count']
                    }
                    logger.info('爬取微博id:{} 的一条微博成功,内容为{}'.format(mid, {

                        "created": item['created_at'],
                        "id": item['id'],

                        'text': item['text_raw'],
                        # 评论内容
                        'comments_count': item['comments_count'],
                        # 转发数量
                        'reposts_count': item['reposts_count'],
                        # 点赞数量
                        'attitudes_count': item['attitudes_count']
                    }))
                    # 将爬取的内容存入到weibo/mid/info.csv里面
            else:
                logger.warning('请求失败，请检查cookie是否过期或者账号是否被封锁')
                print(resp)

    def parse_one_up_blogs(self, mid):
        os.makedirs(os.path.join(self.script_path,
                    'weibo', str(mid)), exist_ok=True)
        blogs = self.crawl_one_up_blogs(mid)
        for blog in blogs:
            # 第一次进来要写表头，其他情况不用写，
            # 将这个博主的信息写入到 ./weibo/{mid}/下面的article.csv文件里面
            with open(os.path.join(self.script_path, 'weibo', str(mid), 'article.csv'), 'a', encoding='utf-8-sig', newline='') as f:
                csv.writer(f).writerow([blog['id'], blog['created'],  blog['text'],
                                        blog['comments_count'], blog['reposts_count'], blog['attitudes_count']])

    def run(self):
        self.parse_user_info()
        for mid in self.user_id_list:
            self.parse_one_up_blogs(mid)
            logger.info("休眠10s~~~")
            sleep(10)

        logger.info("爬取完成")
        return True


if __name__ == "__main__":
    wb = WeiBoCrawl()
    wb.run()
