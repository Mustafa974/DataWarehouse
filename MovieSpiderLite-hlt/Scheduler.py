from Managers import HeaderManager, MongoManager, ProxyManager
from Config import Config
import requests
from requests.exceptions import RequestException
from pyquery import PyQuery as pq
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import re
import datetime
import time
import json


class Scheduler(object):

    def __init__(self, src_col, dest_result_col, tag):
        """
        初始化
        :param src_col:
        :param dest_result_col:
        """
        self.src_col = src_col
        self.dest_col = dest_result_col
        self.tag = tag
        self.mm = MongoManager.DBManager()
        self.pm = ProxyManager.Proxy()
        self.hm = HeaderManager.Headers()
        # self.neo = Neo4j.Neo4j()


    def import_data_from_txt(self, path, dest):
        """
        从txt文件中获取所需数据并存入目标数据库
        :param path:
        :param dest:
        :return:
        """
        f = open(path, 'r', encoding='ISO-8859-1')
        try:
            # 生成迭代器，初始化
            iter_f = iter(f)
            cur_url = ''
            old_url = ''
            count = 0 #记录当前循环次数
            result = []
            userID = ''
            profileName = ''
            helpfulness = ''
            score = ''
            time = ''
            summary = ''
            text = ''

            # 遍历存储全部电影id
            for data in iter_f:
                # if count > 50:
                #     break
                # 若当前行是目标行则执行数据库存储
                if data.find('product/productId') >= 0:
                    # 找到一个新的电影id，循环数加一
                    count += 1
                    cur_url = 'https://www.amazon.com/dp/' + data.replace('product/productId: ', '').strip('\n') + '/'
                    if count is 1:
                        old_url = cur_url
                        continue
                    data = {
                        'userID': userID,
                        'profileName': profileName,
                        'helpfulness': helpfulness,
                        'score': score,
                        'time': time,
                        'summary': summary,
                        'text': text
                    }
                    result.append(data)
                    # 若当前电影id与上一个不同，则应将历史数据打包存入数据库
                    if cur_url != old_url:
                        print('')
                        print('当前电影Id为', cur_url, '上一部电影id为', old_url, '，存储上一部电影的评论结果数据')
                        # 将result存入数据库
                        # for item in result:
                        #     print(item)
                        self.mm.update_attr(self.src_col, old_url, 'reviews', result)
                        result.clear()
                    # 重置旧url
                    old_url = cur_url

                elif data.find('review/userId') >= 0:
                    userID = data.replace('review/userId: ', '').strip('\n')
                elif data.find('review/profileName') >= 0:
                    profileName = data.replace('review/profileName: ', '').strip('\n')
                elif data.find('review/helpfulness') >= 0:
                    helpfulness = data.replace('review/helpfulness: ', '').strip('\n')
                elif data.find('review/score') >= 0:
                    score = data.replace('review/score: ', '').strip('\n')
                elif data.find('review/time') >= 0:
                    time = data.replace('review/time: ', '').strip('\n')
                elif data.find('review/summary') >= 0:
                    summary = data.replace('review/summary: ', '').strip('\n')
                elif data.find('review/text') >= 0:
                    text = data.replace('review/text: ', '').strip('\n')
            # 最后一组数据，循环结束后存入数据库
            data = {
                'userID': userID,
                'profileName': profileName,
                'helpfulness': helpfulness,
                'score': score,
                'time': time,
                'summary': summary,
                'text': text
            }
            result.append(data)
            self.mm.update_attr(self.src_col, old_url, 'reviews', result)
        finally:
            if f:
                # 关闭文件
                f.close()


    def get_page_detail(self, url, headers=None, proxy=''):
        """
        获取详情页信息
        :param url:
        :param headers:
        :return:
        """
        # 禁用安全请求警告
        urllib3.disable_warnings(InsecureRequestWarning)

        response = requests.get(url, headers=headers, proxies=proxy, verify=False)
        try:
            if response.status_code == 200:
                return response.text
            return None
        except RequestException:
            print('请求详情页失败', url)
            return None


    def parse_page(self, url, html, id):
        """
        解析网页信息，存入数据库
        :param url:
        :param html:
        :param id:
        :return bool:如果出现robot则返回False，否则正常页面存入数据库，返回True
        """
        doc = pq(html)
        rated = ''
        type = 'U'


        # 根据影片名CSS选择器类型判断当前页面黑白类型，并存储影片分级
        movie_name = doc('#a-page > div.avu-content.avu-section > div > div > section > h1').text()
        if movie_name != '':
            # print("黑色页面")
            rated = doc(
                '#a-page > div.avu-content.avu-section > div.av-dp-container > div.avu-page-container.avu-clearfix'
                ' > section.av-detail-section > div.av-badges > span.av-badge.av-badge-text').text()
            type = 'B'

        else:
            movie_name = doc('#productTitle').text()
            if movie_name != '':
                # print("白色页面")
                rated = doc('#bylineInfo > div > div > span.a-size-small').text()
                type = 'W'


        # 有明确分级，确定当前产品为电影，直接存入结果数据库
        if rated == 'R' or rated == 'G' or rated == 'PG' or rated == 'PG-13' or rated == 'NC-17':
            print("电影等级为", rated, "，电影名为", movie_name, "，存入结果数据库")
            data = {
                'url': url,
                'movieName': movie_name,
                'type': type
            }
            self.mm.save_data_to_dest(data)

        # 没有明确分级, 存入临时数据库
        else:
            # 获取页面详细信息并存储
            data = []
            if type == 'W':
                details = doc('#detail-bullets .bucket .content li').items()
                if details != '':
                    for item in details:
                        detail = item.text()
                        data.append(detail)

            if type == 'B':
                details = doc('.avu-page-section .a-keyvalue tr').items()
                if details != '':
                    for item in details:
                        detail = item.text()
                        data.append(detail)

            if type == 'U':
                print("##############出现其他页面, 存入临时数据库############")
                data.append({'url':url})
                data.append({'html':html})

            # 确定机器人检查
            page_title = doc('head > title').text()
            if page_title == 'Robot Check':
                print("-------------- Robot -----------------")
                return False

            # 处理结束，存入临时结果数据库
            """
                        if self.mm.save_data_to_temp(data) and self.mm.delete_url(id, self.src_col) and self.mm.delete_url(id, Config.MOVIE_TABLE):
                return True
            """


    def save_page_detail(self, url, id, html, page_type):
        """
        根据html获取网页所需属性并存入dest数据库，同时删去src数据库中的对应数据
        :param html:
        :param page_type:
        :return: 0表示操作完成，1表示遇到机器人检查，2表示出现不存在的页面类型，3表示数据更新操作出错，4表示数据删除失败
        """
        doc = pq(html)
        rating = ''
        actors = ''
        supporting_actors = ''
        directors = ''
        producers = ''
        writers = ''
        format = ''
        genres = ''
        duration = ''
        price = ''
        studio = ''
        release_time = ''
        asin = ''
        star = ''

        # 确定机器人检查
        page_title = doc('head > title').text()
        if page_title == 'Robot Check':
            print("-------------- Robot -----------------")
            return 1

        if page_type != 'W' and page_type != 'B' :
            print("error : 出现不存在的页面类型")
            return 2

        # 根据影片页面类型获取对应属性值
        if page_type == 'W':
            # print("白色页面")
            rating = doc('#bylineInfo > div > div > span.a-size-small').text()
            # print("影片等级为", rating)
            price = doc('#buyNewSection > h5 > div > div.a-column.a-span8.a-text-right.a-span-last > div > span.a-size-medium.a-color-price.offer-price.a-text-normal').text()
            if price == '':
                price = doc('#unqualifiedBuyBox > div > div.a-text-center.a-spacing-mini > span').text()
            # print("影片价格为", price)
            star = doc('#reviewsMedley > div > div.a-fixed-left-grid-col.a-col-left > div.a-section.a-spacing-none.a-spacing-top-mini > div > div > div.a-fixed-left-grid-col.a-col-right > div > span > span > a > span').text()
            # print("评级为", star)
            details = doc('#detail-bullets .bucket .content li').items()
            if details != '':
                for item in details:
                    detail = item.text()
                    # print(detail)
                    if 'Actors' in detail:
                        actors = detail.strip('Actors').strip('\n').strip(':')
                        # print("演员为", actors)
                    if 'Directors' in detail:
                        directors = detail.strip('Directors').strip('\n').strip(':')
                        # print("导演为", directors)
                    if 'Producers' in detail:
                        producers = detail.strip('Producers').strip('\n').strip(':')
                        # print("出品人为", producers)
                    if 'Writers' in detail:
                        writers = detail.strip('Writers').strip('\n').strip(':')
                        # print("原著作者为", writers)
                    if 'Format' in detail:
                        format = detail.strip('Format').strip('\n').strip(':')
                        # print("影片格式为", format)
                    if 'VHS Release Date' in detail:
                        release_time = detail.strip('VHS Release Date').strip('\n').strip(':')
                        # print("上映时间为", release_time)
                    if 'Run Time' in detail:
                        duration = detail.strip('Run Time').strip('\n').strip(':')
                        # print("影片时长为", duration)
                    if 'Studio' in detail:
                        studio = detail.strip('Studio').strip('\n').strip(':')
                        # print("制作公司为", studio)
                    if 'ASIN' in detail:
                        asin = detail.strip('ASIN').strip('\n').strip(':')
                        # print("ASIN为", asin)

        if page_type == 'B':
            # print("黑色页面")
            rating = doc(
                '#a-page > div.avu-content.avu-section > div.av-dp-container > div.avu-page-container.avu-clearfix'
                ' > section.av-detail-section > div.av-badges > span.av-badge.av-badge-text').text()
            # print("影片等级为", rating)
            price = doc('#dv-action-box > div > div.av-action-button-box.avu-full-width-phablet.av-spacer > form:nth-child(2) > button').text().strip('Buy ').strip('HD').strip('SD').strip('\n')
            # print("影片价格为", price)
            release_time = doc('#a-page > div.avu-content.avu-section > div > div > section > div.av-badges > span:nth-child(3)').text()
            # print("上映时间为", release_time)
            duration = doc('#a-page > div.avu-content.avu-section > div > div > section > div.av-badges > span:nth-child(2)').text()
            # print("影片时长为", duration)
            asin = doc('#dv-action-box > div > div.av-action-button-box.avu-full-width-phablet.av-spacer > form:nth-child(1) > input[type="hidden"]:nth-child(4)').val()
            # print("asin为", asin)
            star = doc('#reviewsMedley > div > div.a-fixed-left-grid-col.a-col-left > div.a-section.a-spacing-none.a-spacing-top-mini > div > div > div.a-fixed-left-grid-col.a-col-right > div > span > span > a > span').text()
            # print("评级为", star)
            details = doc('.avu-page-section .a-keyvalue tr').items()
            if details != '':
                for item in details:
                    detail = item.text()
                    # print(detail)
                    if 'Starring' in detail:
                        actors = detail.strip('Starring').strip('\n')
                        # print("演员为", actors)
                    if 'Supporting actors' in detail:
                        supporting_actors = detail.strip('Supporting actors').strip('\n')
                        # print("助演演员为", supporting_actors)
                    if 'Director' in detail:
                        directors = detail.strip('Director').strip('\n')
                        # print("导演为", directors)
                    if 'Producers' in detail:
                        producers = detail.strip('Producers').strip('\n')
                        # print("出品人为", producers)
                    if 'Writers' in detail:
                        writers = detail.strip('Writers').strip('\n')
                        # print("原著作者为", writers)
                    if 'Genres' in detail:
                        genres = detail.strip('Genres').strip('\n')
                        # print("影片类型为", genres)
                    if 'Format' in detail:
                        format = detail.strip('Format').strip('\n')
                        # print("影片格式为", format)
                    if 'Studio' in detail:
                        studio = detail.strip('Studio').strip('\n')
                        # print("制作公司为", studio)

        if rating != '':
            if self.mm.update_attr(self.dest_col, url, 'rating', rating) is False:
                return 3
        if price != '':
            if self.mm.update_attr(self.dest_col, url, 'price', price) is False:
                return 3
        if actors != '':
            if self.mm.update_attr(self.dest_col, url, 'actors', actors) is False:
                return 3
        if supporting_actors != '':
            if self.mm.update_attr(self.dest_col, url, 'supportingActors', supporting_actors) is False:
                return 3
        if directors != '':
            if self.mm.update_attr(self.dest_col, url, 'directors', directors) is False:
                return 3
        if producers != '':
            if self.mm.update_attr(self.dest_col, url, 'producers', producers) is False:
                return 3
        if writers != '':
            if self.mm.update_attr(self.dest_col, url, 'writers', writers) is False:
                return 3
        if format != '':
            if self.mm.update_attr(self.dest_col, url, 'format', format) is False:
                return 3
        if genres != '':
            if self.mm.update_attr(self.dest_col, url, 'genres', genres) is False:
                return 3
        if duration != '':
            if self.mm.update_attr(self.dest_col, url, 'duration', duration) is False:
                return 3
        if studio != '':
            if self.mm.update_attr(self.dest_col, url, 'studio', studio) is False:
                return 3
        if release_time != '':
            if self.mm.update_attr(self.dest_col, url, 'releaseTime', release_time) is False:
                return 3
        if asin != '':
            if self.mm.update_attr(self.dest_col, url, 'asin', asin) is False:
                return 3
        if star != '':
            if self.mm.update_attr(self.dest_col, url, 'star', star) is False:
                return 3

        if self.mm.delete_data(self.src_col, id, url) is False:
            print("删除数据出错")
            return 4

        return 0


    def sort_result_db(self):
        """
        把数据库按序排列
        :return:
        """
        print("开始排序")
        datas = self.mm.sort("movieName", 1)
        for data in datas:
            self.mm.save_data_to_dest(data)


    def bucket_processing(self):
        """
        将所有数据根据首字母分桶，方便后期搜索
        :return:
        """
        datas = self.mm.get_all_data_from_src()
        for data in datas:
            movie_name = data['movieName']
            mn = movie_name.lower()
            i = 0
            if re.match('a.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入a桶，i = ", i)
                continue
            i = i + 1
            if re.match('b.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入b桶，i = ", i)
                continue
            i = i + 1
            if re.match('c.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入c桶，i = ", i)
                continue
            i = i + 1
            if re.match('d.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入d桶，i = ", i)
                continue
            i = i + 1
            if re.match('e.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入e桶，i = ", i)
                continue
            i = i + 1
            if re.match('f.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入f桶，i = ", i)
                continue
            i = i + 1
            if re.match('g.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入g桶，i = ", i)
                continue
            i = i + 1
            if re.match('h.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入h桶，i = ", i)
                continue
            i = i + 1
            if re.match('i.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入i桶，i = ", i)
                continue
            i = i + 1
            if re.match('j.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入j桶，i = ", i)
                continue
            i = i + 1
            if re.match('k.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入k桶，i = ", i)
                continue
            i = i + 1
            if re.match('l.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入l桶，i = ", i)
                continue
            i = i + 1
            if re.match('m.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入m桶，i = ", i)
                continue
            i = i + 1
            if re.match('n.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入n桶，i = ", i)
                continue
            i = i + 1
            if re.match('o.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入o桶，i = ", i)
                continue
            i = i + 1
            if re.match('p.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入p桶，i = ", i)
                continue
            i = i + 1
            if re.match('q.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入q桶，i = ", i)
                continue
            i = i + 1
            if re.match('r.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入r桶，i = ", i)
                continue
            i = i + 1
            if re.match('s.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入s桶，i = ", i)
                continue
            i = i + 1
            if re.match('t.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入t桶，i = ", i)
                continue
            i = i + 1
            if re.match('u.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入u桶，i = ", i)
                continue
            i = i + 1
            if re.match('v.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入v桶，i = ", i)
                continue
            i = i + 1
            if re.match('w.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入w桶，i = ", i)
                continue
            i = i + 1
            if re.match('x.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入x桶，i = ", i)
                continue
            i = i + 1
            if re.match('y.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入y桶，i = ", i)
                continue
            i = i + 1
            if re.match('z.*', mn):
                self.mm.save_data(data, Config.TABLES[i])
                print("存入z桶，i = ", i)
                continue
            i = i + 1
            self.mm.save_data(data, Config.TABLES[i])
            print("存入#桶，i = ", i)


    def data_cleaning(self):
        """
        数据清洗
        :return:
        """
        datas = self.mm.get_all_data_from_src()
        formal_name = ''
        formal_url = ''
        formal_id = ''
        count = 0
        for data in datas:

            # 将电影名去掉空格、特殊符号，替换为小写，替换特殊字符
            current_name = data['movieName']
            cn = current_name.replace(' I ', '1').replace('II', '2').replace('III', '3').lower().replace('the', '').replace('and', '&').replace('[', '').replace(']', '').replace('/', '').replace('-', '').replace('~', '').replace('(', '').replace(')', '').replace(':', '').replace('\'', '').replace('vs.', 'versus').replace('.', '').replace(',', '').replace('!', '').replace('?', '')

            """
            fn = formal_name.replace(' ', '').lower().replace('and', '&').replace('[', '').replace(']', '').replace('/', '').replace('-', '').replace('~', '').replace('(', '').replace(')', '').replace(':', '')
            result = cn.replace(fn, '')
            if result == 'vhs':
                print("VHS")
                print(formal_name)
                print(current_name)
                self.mm.delete_data(data['_id'], current_name, Config.MOVIE_TABLE)
                count = count + 1
            formal_name = current_name
            formal_url = data['url']
            formal_id = data['_id']
            """

            result = re.match('.*(tv ).*', cn)
            if result is not None:
                print("匹配到 baby lesson")
                print(data['url'], current_name)
                #self.mm.delete_data(data['_id'], data['movieName'], Config.MOVIE_TABLE)
                count = count + 1

                """
                result = self.mm.delete_data_ignor_case(cn.replace('vhs', '').replace(' ', ''), data['url'])
                if result is not None:
                    #print(data['url'], data['movieName'])
                    print("目标电影：", result['url'], result['movieName'])
                    print("当前电影：", data['url'], data['movieName'])
                    self.mm.delete_data(data['_id'], data['movieName'], Config.MOVIE_TABLE)
                    print('############')
                    count = count + 1
                """

        print('')
        print("删除数据总数", count)


    def write_to_file(self, path, content):
        """
        将参数中的结果写入txt文件
        :return:
        """
        with open(path, 'a', encoding='utf-8') as f:
            # 清除txt文件
            # f.truncate()
            f.write(json.dumps(content, ensure_ascii=False) + '\n')
            f.close()


    def write_result_to_txt(self):
        """
        将结果电影名存入txt
        :return:
        """
        datas = self.mm.get_data()
        for data in datas:
            print(data)
            self.write_to_file(data['movieName'])


    def split_arr(self, _data):
        """
        处理演员和导演名
        按照逗号分割
        去除空格，替换换行符
        :param _data:
        :return:
        """
        results = []
        datas = re.split(',', _data)
        for data in datas:
            data = data.strip(' ').replace('\n', ' ')
            results.append(data)
        return results


    def add_relation_count(self, tuples, director, actors):
        count = tuples.count()

        # 没有此导演
        if count == 0:
            # 所有合作演员全部存储
            for actor in actors:
                data = {
                    'director': director,
                    'actor': actor,
                    'count': 1
                }
                self.mm.save_data(self.dest_col, data)
            return 0
        # 有此导演
        for tuple in tuples:
            if tuple['actor'] in actors:
                actors.remove(tuple['actor'])
                self.mm.update_attr(self.dest_col, tuple['_id'], 'count', tuple['count'] + 1)
            # 剩余的演员是该导演未合作过的，插入新数据项
            # print(actors)
        for actor in actors:
            data = {
                'director': director,
                'actor': actor,
                'count': 1
            }
            self.mm.save_data(self.dest_col, data)
        return 1


    def save_cooperate(self):
        """
        使用mongodb
        将信息数据库中的导演与演员一一拆分
        存入cooperation_list数据库
        :return:
        """
        datas = self.mm.get_data(self.src_col)
        count = 1
        for data in datas:
            print(count, '#####')
            if 'actors' in data and 'directors' in data:
                directors = self.process_act_dir(data['directors'])
                actors = self.process_act_dir(data['actors'])
                for director in directors:
                    tuples = self.mm.find_data(self.dest_col, director)
                    self.add_relation_count(tuples, director, actors)
            else:
                print('no actor or no directors')
            count += 1


    def save_cooperate_by_neo(self):
        """
        使用neo4j数据库
        将mongodb数据库中的信息图形化存储
        :return:
        """
        datas = self.mm.get_data(self.src_col)
        count = 1
        for data in datas:
            print(count, '-----')
            # if count > 50:
            #     break

            # 该条数据有导演和演员信息，进行处理
            if 'actors' in data and 'directors' in data:
                # 数据处理，去掉空格，替换换行符
                directors = self.process_act_dir(data['directors'])
                actors = self.process_act_dir(data['actors'])
                # 对于数据中的全部导演，一一进行处理
                for director in directors:
                    # 图中没有找到导演节点，针对该节点，创建与每个导演的关系
                    if self.neo.find_director_node(director) is False:
                        for actor in actors:
                            self.neo.add_relation(director, actor)
                    # 图中找到导演节点
                    else:
                        # 针对每个演员进行处理
                        for actor in actors:
                            # 有导演节点，没有演员节点，直接创建关系
                            if self.neo.find_actor_node(actor) is False:
                                self.neo.add_relation(director, actor)
                            # 导演和演员节点同时存在，查找关系并合并到图中
                            else:
                                print('dir and act both exist')
                                self.neo.find_relation_and_add_count(director, actor)
            # 该条数据没有导演或演员信息，直接跳过
            else:
                print('no actor or no directors')
            count += 1


    def save_coop_result_to_mongo(self):
        """
        将neo4j中的数据结果存入mongodb
        :return:
        """
        datas = self.neo.get_cooperations()
        if datas is None:
            print('Error!')
            return False
        count = 1
        for data in datas:
            if count > 10:
                break
            print(data)
            # self.mm.save_data(self.dest_col, data)
        return True


    def schedule(self):
        """
        总调度函数，爬取网页信息并存储到mongodb
        :return:
        """
        # 从目标数据库获取源数据
        datas = self.mm.get_data(self.src_col)

        # 记录当前爬取次数
        count = 1

        # 获取对应申请头
        headers = self.hm.get_headers(self.tag)

        ## 获取一个可用代理
        proxy = self.pm.get_proxy()
        while proxy is None:
            print("1 代理请求失败")
            proxy = self.pm.get_proxy()

        for i in range(0, Config.SPIDER_TIME):

            data = datas[i]
            # 打印调试信息
            print('\nspidering :', data['url'], "，当前解析页数：", count, " ", self.src_col)

            # 当前需要解析网页
            while True:
                print("当前代理", proxy)
                html = self.get_page_detail(data['url'], headers, proxy)

                # 页面出现404，从src数据库删除并存入test_404数据库，跳出循环，结束本次url查询
                if html is None:
                    print("!!!!!!!!!!!!!!!!!!!!!!!页面出现404!!!!!!!!!!!!!!!!!!!!!!!！")
                    if self.mm.delete_data(self.src_col, data['_id'], data['url']) and self.mm.save_data(
                            Config.TEST_404, data):
                        print("删除404数据，存入404数据库")
                    break

                # 解析html，将需要的信息存入数据库，删除源数据
                result = self.save_page_detail(data['url'], data['_id'], html, data['type'])
                # 遇到机器人，需要更换代理继续
                if result == 1:
                    print("-----更换代理-----")
                    proxy = self.pm.get_proxy()
                    while proxy is None:
                        print("2 代理请求失败")
                        proxy = self.pm.get_proxy()
                # 出现特殊页面
                if result == 2:
                    print("!!!!!!!!!!!!!!!!!!!!!!!出现特殊页面!!!!!!!!!!!!!!!!!!!!!!!！")
                    if self.mm.delete_data(self.src_col, data['_id'], data['url']) and self.mm.save_data(
                            Config.TEST_404, data):
                        print("删除特殊页面数据，存入404数据库")
                    break
                # 数据库操作出错，跳过当前页面
                if result == 3 or result == 4:
                    print("数据库操作出错")
                    break
                # 操作成功，不需要继续解析网页，跳出循环
                else:
                    break

            count += 1
            time.sleep(Config.SLEEP_TIME)


    def cooperations_processing(self):
        datas = self.mm.get_data(Config.COOPERATION_LIST)
        count = 0
        time = 0
        for data in datas:
            director = data['director']
            actor = data['actor']
            # if len(director) < 2 or len(actor) < 2:
            #     print('director:', director, 'actor:', actor)
            #     count += 1
                # self.mm.delete_data(Config.COOPERATION_LIST, data['_id'])
            if data['count'] > 1:
                row = {
                    'director': data['director'],
                    'actor': data['actor'],
                    'count': data['count']
                }
                self.write_to_file('CooperationCount.txt', row)
                time += 1
        print('count:', count, 'time:', time)
        print('database:', self.mm.get_count(Config.COOPERATION_LIST))


    def split_processing(self):
        datas = self.mm.get_data(self.src_col)
        count = 1
        attrs = [
            'actors',
            'producers',
            'genres',
            'directors',
            'writers',
            'studio',
            'supportingActors',
            'format'
        ]
        for data in datas:
            print(count, '----------------')
            # if count > 1:
            #     break
            for attr in attrs:
                if attr in data:
                    if isinstance(data[attr], list):
                        continue
                    if data[attr] is None:
                        continue
                    result = self.split_arr(data[attr])
                    self.mm.update_attr(self.src_col, data['_id'], attr, result)
            count += 1


    def run(self):
        start = datetime.datetime.now()


        end = datetime.datetime.now()
        print("调度总时长：", end - start)

