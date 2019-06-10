import sqlite3
import re
from bs4 import BeautifulSoup
from datetime import date
import csv


class CsvReport:
    select_all_rqst = 'SELECT * FROM links'

    def __init__(self, db_path, report_path='report.csv'):
        self.db = sqlite3.connect(db_path)
        self.cursor = self.db.cursor()
        self.path = report_path

    def create(self):
        all_links = self.cursor.execute(self.select_all_rqst).fetchall()
        if all_links:
            with open(self.path, 'w') as csv_report:
                writer = csv.writer(csv_report, delimiter=';')
                for link in all_links:
                    writer.writerow(link)


class HtmlReport:
    select_start_url = 'SELECT url FROM links WHERE text="Start Page"'
    select_invalid_rqst = 'SELECT * FROM links WHERE status_code>=400 AND url NOT REGEXP ?'
    select_404 = 'SELECT text, url, referer_url FROM links WHERE status_code=404 AND url NOT REGEXP ?'
    select_401 = 'SELECT text, url, referer_url FROM links WHERE status_code=401 AND url NOT REGEXP ?'
    select_4xx = 'SELECT * FROM links ' \
                 'WHERE status_code>=400 ' \
                 'AND status_code<500 ' \
                 'AND status_code<>404 ' \
                 'AND status_code<>401 ' \
                 'AND url NOT REGEXP ?'
    select_5xx = 'SELECT * FROM links WHERE status_code>=500 AND url NOT REGEXP ?'
    select_exceptions = 'SELECT * FROM links WHERE url REGEXP ?'
    select_group_rqst = 'SELECT status_code, ' \
                        'COUNT(url), ' \
                        'ROUND(COUNT(url) * 100.00/(SELECT COUNT(url) FROM links), 2) ' \
                        'FROM links ' \
                        'GROUP BY status_code'
    select_total_rqst = 'SELECT COUNT(url) FROM links'
    thead_cells = ['Link Text', 'URL', 'Referrer URL', 'Status Code']

    def __init__(self, db_path, report_path='report.html', exception=['']):
        self.db = sqlite3.connect(db_path)
        self.db.create_function('REGEXP', 2, lambda pattern, url: 1 if pattern and re.search(pattern, url) else 0)
        self.exception = exception
        self.cursor = self.db.cursor()
        self.report = BeautifulSoup(features='lxml')
        self.path = report_path

    def create_structure(self):
        html = self.report.new_tag('html')
        head = self.report.new_tag('head')
        body = self.report.new_tag('body')
        css_link = self.report.new_tag('link', href='styles.css', rel='stylesheet')
        self.report.append(html)
        html.append(head)
        html.append(body)
        head.append(css_link)

    def h(self, level, text, **kwargs):
        h = self.report.new_tag('h%s' % level, **kwargs)
        h.string = text
        return h

    def a(self, url, **kwargs):
        a = self.report.new_tag('a', href=url, **kwargs)
        a.string = url
        return a

    def thead(self, cells, **kwargs):
        thead = self.report.new_tag('thead', **kwargs)
        for cell in cells:
            thead.append(self.td(cell))
        return thead

    def td(self, text=None, **kwargs):
        td = self.report.new_tag('td', **kwargs)
        if text:
            td.string = str(text)
        return td

    def tr(self, cells, **kwargs):
        tr = self.report.new_tag('tr', **kwargs)
        for cell in cells:
            if str(cell).startswith('http'):
                td = self.td()
                td.append(self.a(cell))
            else:
                td = self.td(cell)
            tr.append(td)
        return tr

    def table(self, caption_text, thead_cells, links, **kwargs):
        # создаем таблицу
        table = self.report.new_tag('table', **kwargs)
        caption = self.report.new_tag('caption')
        caption.string = caption_text
        table.append(caption)
        # добавляем thead
        table.append(self.thead(thead_cells))
        # добавляем строки
        for link in links:
            table.append(self.tr(link))
        return table

    def append_table(self, rqst, caption, theads):
        links = self.cursor.execute(rqst, self.exception).fetchall()
        if links:
            self.report.body.append(self.table(caption, theads, links))

    def create(self):
        self.create_structure()
        self.report.body.append(self.h(1, 'Broken link report'))
        self.report.body.append(self.h(2, 'Created: %s' % date.today().strftime('%d.%m.%Y')))
        start_url = self.cursor.execute(self.select_start_url).fetchone()[0]
        start_url_h3 = self.h(3, 'Start URL:')
        start_url_h3.append(self.a(start_url))
        self.report.body.append(start_url_h3)

        # сводная таблица
        grouped_links = self.cursor.execute(self.select_group_rqst).fetchall()
        total = self.cursor.execute(self.select_total_rqst).fetchone()[0]
        grouped_links.append(('Total', total, '100.00'))
        self.report.body.append(self.table('Links by status', ['Status', 'Count', '%'], grouped_links))

        # 404
        self.append_table(self.select_404, 'Not found (404)', self.thead_cells[:-1])

        # 401
        self.append_table(self.select_401, 'Auth required (401)', self.thead_cells[:-1])

        # 4xx
        self.append_table(self.select_4xx, 'Client errors', self.thead_cells)

        # 5xx
        self.append_table(self.select_5xx, 'Server errors', self.thead_cells)

        # exceptions
        self.append_table(self.select_exceptions, 'Exceptions', self.thead_cells)

        # invalid_links = self.cursor.execute(self.select_invalid_rqst).fetchall()
        # if invalid_links:
        #     self.report.body.append(self.table('Invalid Links', self.thead_cells, invalid_links))
        #
        # all_links = self.cursor.execute(self.select_all_rqst).fetchall()
        # self.report.body.append(self.table('Visited Links', self.thead_cells, all_links))

        with open(self.path, 'w') as report_file:
            report_file.write(self.report.prettify())
