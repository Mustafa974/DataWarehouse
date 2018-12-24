from config import *
from pyquery import PyQuery as pq
import re


class ParsePage:
    """
    解析页面
    """
    def parse_page_fully(self, doc, page=-1):
        if page == WHITE:
            return self.parse_white_page_fully(doc)
        elif page == BLACK:
            return self.parse_black_page_fully(doc)
        else:
            lang = doc('html').attr('lang')
            if lang is None:
                # for black page
                return self.parse_black_page_fully(doc)
            else:
                # for white page
                return self.parse_white_page_fully(doc)

    @staticmethod
    def get_page_type(doc):
        """
        判断页面类型
        :param doc:
        :return:
        """
        lang = doc('html').attr('lang')
        if lang is None:
            # for 404 page
            if doc('body #g #detail_all_table').attr('alt') == 'Dogs of Amazon' \
                    or doc('body #g #d').attr('alt') == 'Dogs of Amazon':
                return DOG
            # for black page
            else:
                return BLACK
        else:
            # for robot check
            if doc('head title').text() == 'Robot Check':
                return ROBOT
            # for white page
            return WHITE

    @staticmethod
    def parse_white_page_fully(doc):
        """
        解析白色页面
        :param doc:
        :return:
        """
        item = {'title': doc('#dp-container #centerCol #productTitle').text()}
        details = doc('#detail-bullets .content ul li')
        for li in details:
            pqli = pq(li)
            if pqli('li b').text() == 'Actors:':
                actors = []
                for actor in pqli('a'):
                    actors.append(pq(actor).text())
                item['actors'] = actors
            elif pqli('b').text() == 'Directors:':
                directors = []
                for director in pqli('a'):
                    directors.append(pq(director).text())
                item['directors'] = directors
            elif pqli('b').text().strip(' ') == 'Rated:':
                rated = pqli('.a-box-inner .a-size-small').text()
                item['rated'] = rated
            elif 'Release Date' in pqli('b').text():
                item[TIME] = pqli.text().replace(pqli('b').text(), '').strip(' ')
            elif pqli('b').text() == 'Average Customer Review:':
                review = pqli('.a-size-small a').text().replace('customer review', '').replace('s', '').strip(' ')
                try:
                    item[REVIEW] = int(review)
                except ValueError:
                    pass
        return item

    @staticmethod
    def parse_black_page_fully(doc):
        """
        解析黑色页面
        :param doc:
        :return:
        """
        head = pq(doc('#a-page .av-dp-container .av-detail-section'))
        try:
            review = int(head('.av-badge-text a').text().strip('(').strip(')').replace(',', ''))
        except ValueError:
            review = None
        item = {
            TITLE: head('h1').text(),
            REVIEW: review,
            TIME: head('.av-badge-text[data-automation-id="release-year-badge"]').text()
        }
        details = doc(
            '#a-page .avu-content .avu-section-alt .avu-page-section .aiv-wrapper .aiv-container-limited table tr')
        for each in details:
            tr = pq(each)
            if tr('th').text().find('Genres') != -1:
                genres = []
                for a in tr('td a'):
                    genres.append(pq(a).text())
                item['genres'] = genres
            elif tr('th').text().find('Director') != -1:
                directors = []
                for a in tr('td a'):
                    directors.append(pq(a).text())
                item['directors'] = directors
            elif tr('th').text().find('Starring') != -1:
                actors = []
                for a in tr('td a'):
                    actors.append(pq(a).text())
                item['actors'] = actors
            elif tr('th').text().find('MPAA rating') != -1:
                rated = tr('td').text().strip(' ')
                pattern = r'\(.*?\)'
                rated = re.sub(pattern, '', rated).strip(' ')
                item['rated'] = rated
        return item

