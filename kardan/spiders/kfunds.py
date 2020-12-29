import scrapy
import pandas as pd 
import numpy as np 
import persian

def fa_to_eng(series):
    return series.aggregate(lambda x: persian.convert_fa_numbers(x) \
        if x[0] != '(' else -1*float(persian.convert_fa_numbers(x.strip('()'))))

def remove_comma(series):
    return series.str.replace(',', '', regex=False)


class KfundsSpider(scrapy.Spider):
    name = 'kfunds'
    allowed_domains = ['iran-kfunds1.ir/']
    start_urls = ['http://iran-kfunds1.ir/Reports/FundDailyEfficiency/']

    def parse(self, response):

        table = response.xpath('//*[@class="table  m-0"]')
        headers = table.xpath('thead/tr/th/text()').extract()
        li = table.xpath('tbody/tr/td').extract()
        newli = [i.strip('<td> <span class="negative"></span></td>') for i in li]
        reshaped_li = np.reshape(newli, (len(newli)//6,-1))
        df = pd.DataFrame(reshaped_li, columns=headers)

        df['بازده روزانه صندوق'] = fa_to_eng(df['بازده روزانه صندوق'])
        df['بازدهی سالانه شده صندوق'] = fa_to_eng(df['بازدهی سالانه شده صندوق'])
        df['ردیف'] = fa_to_eng(df['ردیف'])
        df['تاریخ'] = fa_to_eng(df['تاریخ'])

        df['بازدهی روزانه شاخص'] = fa_to_eng(df['بازدهی روزانه شاخص'])
        df['بازدهی سالانه شده شاخص'] = fa_to_eng(df['بازدهی سالانه شده شاخص'])

        df = df.astype(str)
        df['بازدهی روزانه شاخص'] = remove_comma(df['بازدهی روزانه شاخص'])
        df['بازدهی سالانه شده شاخص'] = remove_comma(df['بازدهی سالانه شده شاخص'])
        df['بازده روزانه صندوق'] = remove_comma(df['بازده روزانه صندوق'])
        df['بازدهی سالانه شده صندوق'] = remove_comma(df['بازدهی سالانه شده صندوق'])
        df[df.columns[2:]] = df[df.columns[2:]].astype('float')

        abs_url = response.xpath('//*[@class="pager"]/a/@href')[-1].extract()
        next_url = response.urljoin(abs_url)

        current = int(response.xpath('//*[@class="current"]/text()').extract_first())
        last = int(response.xpath('//*[@class="pager"]/a/text()')[-2].extract())

        yield df.to_csv('kardan_dataset.csv', index=False, mode='a', header=False)
        if  response.xpath('//*[@class="pager"]/span[@class="disabled"]') and current > 1:
            return
        else:
            yield scrapy.Request(next_url, callback=self.parse, dont_filter=True)
        
