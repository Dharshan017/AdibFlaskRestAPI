from flask import Blueprint,request,jsonify
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import func
from sqlalchemy.engine.reflection import Inspector
from datetime import datetime,timedelta
import json
from . import db

insights = Blueprint('insights', __name__)

class GetSchema:
    flag = 0
    Base = automap_base()


def prebBaseObject():
    if not GetSchema.flag:
        GetSchema.Base.prepare(db.engine, reflect=True)
        GetSchema.Base.metadata.reflect(db.engine,views=True)

@insights.route('/rank/<int:rnk_>', methods=['GET'])
def rank(rnk_):
    if request.method == 'GET':
        prebBaseObject()
        NBP_FINAL = GetSchema.Base.metadata.tables["nbp_final"]
        # rankschema = RankSchema()

        sub_query = db.session.query(func.count(NBP_FINAL.c.rim_no)).filter(NBP_FINAL.c.rnk == rnk_).scalar_subquery()

        query = db.session.query(
        NBP_FINAL.c.rnk,NBP_FINAL.c.product_acct_type.label('product_name'),func.count(NBP_FINAL.c.rim_no)
        ,(func.count(NBP_FINAL.c.rim_no)/sub_query)*100).group_by(NBP_FINAL.c.rnk, NBP_FINAL.c.product_acct_type).where(NBP_FINAL.c.rnk==rnk_)
        res = db.session.execute(query).all()
        res = [{'rnk':rnk, 'product_name':product_name,'total_count':float(total_count),'percent':float(percent)} for rnk,product_name,total_count,percent in res]
        return json.dumps(res)
    
@insights.route('/retrieveRank', methods=['GET'])
def retrieveRank():
    if request.method == 'GET':
        prebBaseObject()
        NBP_FINAL = GetSchema.Base.metadata.tables["nbp_final"]
        NBP_DS_CUSTOMER = GetSchema.Base.metadata.tables["nbp_ds_customer"]
    
        # query = db.session.query(
        # NBP_FINAL.c.rim_no,NBP_FINAL.c.rnk,NBP_FINAL.c.product_acct_type.label('product_name'))
        query = db.session.query(NBP_FINAL.c.rim_no,NBP_FINAL.c.rnk,NBP_FINAL.c.product_acct_type,
            NBP_DS_CUSTOMER.c.age_bracket,NBP_DS_CUSTOMER.c.salary_bracket,NBP_DS_CUSTOMER.c.region).join(NBP_DS_CUSTOMER,NBP_DS_CUSTOMER.c.rim_no==NBP_FINAL.c.rim_no)
        res = db.session.execute(query).all()
        print(len(res))
        return jsonify([dict(rows) for rows in res])

@insights.route('/productPerformance', methods=['POST'])
def productPerformance():
    if request.method == 'POST':
        prebBaseObject()
        NBP_PS = GetSchema.Base.metadata.tables["nbp_product_status"]

        # start_date = datetime(day=20,month=10,year=2022)
        # end_date = datetime(day=20,month=2,year=2023)
        dates = request.json
        start_date = datetime.strptime(dates["start_date"],"%Y-%m-%d")
        end_date = datetime.strptime(dates["end_date"],"%Y-%m-%d")

        product_count = func.count(NBP_PS.c.rim_no).label('product_count')
        total_count = db.session.query(func.count(NBP_PS.c.rim_no)).where(
            NBP_PS.c.open_date > start_date,
            NBP_PS.c.open_date < end_date
        ).as_scalar()#.label('total_count')
        
        product_percent = (product_count / total_count) * 100#.label('product_percent')

        query = db.session.query(
            NBP_PS.c.product_name,
            product_count,
            product_percent
        ).group_by(NBP_PS.c.product_name).where(
            NBP_PS.c.open_date>start_date,
            NBP_PS.c.open_date<end_date
        )
        res = db.session.execute(query).all()
        res = [{'product_name':product_name, 'product_count':float(product_count),'product_percent':float(product_percent)} for product_name,product_count,product_percent in res]
        return json.dumps(res)
    

@insights.route('/productProfitability', methods=['POST'])
def productProfitability():
    if request.method == 'POST':
        prebBaseObject()
        NBP_PP = GetSchema.Base.metadata.tables["nbp_product_status"]

        dates = request.json
        start_date = datetime.strptime(dates["start_date"],"%Y-%m-%d")
        end_date = datetime.strptime(dates["end_date"],"%Y-%m-%d")

        sum_profit1 = func.sum(NBP_PP.c.profit)
        sum_profit2 = db.session.query(func.sum(NBP_PP.c.profit)).where(
            NBP_PP.c.open_date > start_date,
            NBP_PP.c.open_date < end_date
        ).as_scalar()

        query = db.session.query(
            NBP_PP.c.product_name,
            sum_profit1,
            (sum_profit1/sum_profit2)*100,
        ).group_by(NBP_PP.c.product_name).where(
            NBP_PP.c.open_date>start_date,
            NBP_PP.c.open_date<end_date
        )
        res = db.session.execute(query).all()
        res = [{'product_name':product_name, 'profit':float(profit),'profitability_percent':float(profitability_percent)} for product_name,profit,profitability_percent in res]
        return json.dumps(res)
    

@insights.route('/salesByChannel', methods=['POST'])
def salesByChannel():
    if request.method == 'POST':
        prebBaseObject()
        NBP_PS = GetSchema.Base.metadata.tables["nbp_product_status"]

        dates = request.json
        start_date = datetime.strptime(dates["start_date"],"%Y-%m-%d")
        end_date = datetime.strptime(dates["end_date"],"%Y-%m-%d")

        count_channel1 = func.count(NBP_PS.c.rim_no)
        count_channel2 = db.session.query(func.count(NBP_PS.c.rim_no)).where(
            NBP_PS.c.open_date > start_date,
            NBP_PS.c.open_date < end_date
        ).as_scalar()

        query = db.session.query(
            NBP_PS.c.channel,
            count_channel1,
            (count_channel1/count_channel2)*100,
        ).group_by(NBP_PS.c.channel).where(
            NBP_PS.c.open_date>start_date,
            NBP_PS.c.open_date<end_date
        )
        res = db.session.execute(query).all()
        res = [{'channel': channel, 'countOfChannel': countOfChannel,'usage_percent':float(usage_percent)} for channel,countOfChannel,usage_percent in res]
        return json.dumps(res)
    
# @insights.route('/customerRetention', methods=['POST'])
# def customerRetention():
#     if request.method == 'POST':
#         prebBaseObject()
#         NBP_PS = GetSchema.Base.metadata.tables["nbp_product_status"]

#         dates = request.json
#         start_date = datetime.strptime(dates["start_date"],"%Y-%m-%d")
#         end_date = datetime.strptime(dates["end_date"],"%Y-%m-%d")


#         total_count = db.session.query(func.count(NBP_PS.c.rim_no)).where(NBP_PS.c.open_date > start_date,
#                                               NBP_PS.c.open_date < end_date).as_scalar()
        
#         subquery = db.session.query(NBP_PS.c.rim_no,NBP_PS.c.product_name,
#                                 func.max(NBP_PS.c.last_transaction_date).label('last_transaction_date'),
#                                 func.min(NBP_PS.c.open_date).label('open_date'),
#                                 NBP_PS.c.status) \
#                          .where(NBP_PS.c.open_date > start_date,
#                                  NBP_PS.c.open_date < end_date) \
#                          .group_by(NBP_PS.c.rim_no, NBP_PS.c.product_name, NBP_PS.c.status) \
#                          .subquery()
#         ltd_sub = subquery.c.last_transaction_date
#         uCase = db.case([(ltd_sub - subquery.c.open_date > timedelta(days=365), 'medium'),
#                                       (ltd_sub - subquery.c.open_date > timedelta(days=730), 'long')],
#                                      else_='short').label('usage_duration_range')
#         query = db.session.query(
#             subquery.c.product_name,uCase,
#             (func.count(subquery.c.product_name)/total_count)*100
#             ).group_by("product_name","usage_duration_range")

#         res = db.session.execute(query).all()
#         res = [{'product_name': product_name, 'usage_direction_range': usage_direction_range,'usage_percent':float(usage_percent)} for product_name,usage_direction_range,usage_percent in res]
#         return json.dumps(res)

@insights.route('/customerRetention', methods=['GET'])
def customerRetention():
    if request.method == 'GET':
        prebBaseObject()
        NBP_PS = GetSchema.Base.metadata.tables["nbp_product_status"]

        diff_subquery = db.session.query(
                                NBP_PS.c.product_name,
                                func.ifnull(NBP_PS.c.closed_date, func.current_date()).label('closed_date'),
                                (func.datediff(func.ifnull(NBP_PS.c.closed_date, func.current_date()), NBP_PS.c.open_date)).label('difference')
                            ).subquery().alias('diff_subquery')

        usage_subquery = db.session.query(
                                diff_subquery.c.product_name,
                                db.case(
                                    [
                                        ((diff_subquery.c.difference >= 8) & (diff_subquery.c.difference < 100), 'Short'),
                                        ((diff_subquery.c.difference >= 100) & (diff_subquery.c.difference < 300), 'Medium'),
                                        ((diff_subquery.c.difference >= 300), 'Long'),
                                    ],
                                    else_='VeryLow'
                                ).label('usage_category')
                            ).subquery().alias('usage_subquery')

        main_query = db.session.query(
                            usage_subquery.c.product_name,
                            usage_subquery.c.usage_category,
                            func.count().label('cnt'),
                            func.round(
                                    func.count() / func.sum(func.count()).over(partition_by=usage_subquery.c.product_name) * 100,
                                    2
                                ).label('cnt_percentage')
                            # func.concat(
                            #     func.round(
                            #         func.count() / func.sum(func.count()).over(partition_by=usage_subquery.c.product_name) * 100,
                            #         2
                            #     ),
                            #     '%'
                            # ).label('cnt_percentage')
                        ).group_by(
                                    usage_subquery.c.product_name,
                                    usage_subquery.c.usage_category
                                ).order_by(
                                    usage_subquery.c.product_name
                                )

        res = db.session.execute(main_query).all()
        res = [{'product_name': product_name, 'usage_category': usage_category,'cnt':float(cnt),'cnt_percentage':float(cnt_percentage)} for product_name,usage_category,cnt,cnt_percentage in res]
        return json.dumps(res)
    


@insights.route('/customerDemographics/<int:rnk_>/<string:product_>', methods=['GET'])
def customerDemographics(rnk_,product_):
    if request.method == 'GET':
        prebBaseObject()
        # print(GetSchema.Base.metadata.tables.keys())
        NBP_CD = GetSchema.Base.metadata.tables[f"nbp_customer_segmentation_{rnk_}_{str.lower(product_)}"]

        total_count = db.session.query(func.count(NBP_CD.c.rim_no)).as_scalar()
        
        query = db.session.query(
            NBP_CD.c.region,
            func.count(NBP_CD.c.rim_no),
            (func.count(NBP_CD.c.rim_no)/total_count)*100
            ).group_by(NBP_CD.c.region)

        res = db.session.execute(query).all()
        res = [{'branch_region':branch_region, 'customer_count':float(customer_count) ,'percentage_distribution':float(percentage_distribution)} for branch_region,customer_count,percentage_distribution in res]
        return json.dumps(res)
    
@insights.route('/customerSalaryBracket/<int:rnk_>/<string:product_>', methods=['GET'])
def customerSalaryBracket(rnk_,product_):
    if request.method == 'GET':
        prebBaseObject()
        NBP_CD = GetSchema.Base.metadata.tables[f"nbp_customer_segmentation_{rnk_}_{str.lower(product_)}"]

        total_count = db.session.query(func.count(NBP_CD.c.rim_no)).as_scalar()
        
        query = db.session.query(
            NBP_CD.c.salary_bracket,
            (func.count(NBP_CD.c.rim_no)/total_count)*100
            ).group_by(NBP_CD.c.salary_bracket)

        res = db.session.execute(query).all()
        res = [{'salary_bracket':salary_bracket,'percentage_distribution':float(percentage_distribution)} for salary_bracket,percentage_distribution in res]
        return json.dumps(res)
    
@insights.route('/customerAgeBracket/<int:rnk_>/<string:product_>', methods=['GET'])
def customerAgeBracket(rnk_,product_):
    if request.method == 'GET':
        prebBaseObject()
        NBP_CD = GetSchema.Base.metadata.tables[f"nbp_customer_segmentation_{rnk_}_{str.lower(product_)}"]

        total_count = db.session.query(func.count(NBP_CD.c.rim_no)).as_scalar()
        
        query = db.session.query(
            NBP_CD.c.age_bracket,
            (func.count(NBP_CD.c.rim_no)/total_count)*100
            ).group_by(NBP_CD.c.age_bracket)

        res = db.session.execute(query).all()
        res = [{'age_bracket':age_bracket,'percentage_distribution':float(percentage_distribution)} for age_bracket,percentage_distribution in res]
        return json.dumps(res)
    

@insights.route('/customerTransactionChannel/<int:rnk_>/<string:product_>', methods=['GET'])
def customerTransactionChannel(rnk_,product_):
    if request.method == 'GET':
        prebBaseObject()
        NBP_CD = GetSchema.Base.metadata.tables[f"nbp_customer_transaction_{rnk_}_{str.lower(product_)}"]

        total_count = db.session.query(func.count(NBP_CD.c.rim_no)).as_scalar()
        
        query = db.session.query(
            NBP_CD.c.transaction_channel,
            (func.count(NBP_CD.c.rim_no)/total_count)*100
            ).group_by(NBP_CD.c.transaction_channel)

        res = db.session.execute(query).all()
        res = [{'transaction_channel':transaction_channel,'percentage_distribution':float(percentage_distribution)} for transaction_channel,percentage_distribution in res]
        return json.dumps(res)




@insights.route('/customerTransactionVolume/<int:rnk_>/<string:product_>', methods=['GET'])
def customerTransactionVolume(rnk_,product_):
    if request.method == 'GET':
        prebBaseObject()
        NBP_CD = GetSchema.Base.metadata.tables[f"nbp_customer_transaction_{rnk_}_{str.lower(product_)}"]

        
        tc =  db.session.query(func.count(func.distinct(NBP_CD.c.rim_no))).scalar()
        subquery = db.session.query(
                        NBP_CD.c.rim_no.label('rim_no'),
                        func.count(NBP_CD.c.transaction_amount).label('tot_tran_volume')
                    ).group_by(NBP_CD.c.rim_no).subquery()

        tran_volume_category = db.case(
            [
                (subquery.c.tot_tran_volume.between(50, 89), 'Low'),
                (subquery.c.tot_tran_volume.between(90, 299), 'Medium'),
                (subquery.c.tot_tran_volume >= 300, 'High')
            ],
            else_='VeryLow'
        ).label('tran_volume_category')
        
        s2 = db.session.query(subquery.c.rim_no,tran_volume_category).subquery()
        
        query = db.session.query(
                s2.c.tran_volume_category,
                func.count(s2.c.tran_volume_category).label('cust_cnt'),
                func.round((func.count(s2.c.tran_volume_category) / tc) * 100, 2).label('cust_distribution')
            ).group_by(
                tran_volume_category
            )
        res = db.session.execute(query).all()
        res = [{'tran_volume_category':tran_volume_category, 'cust_cnt':float(cust_cnt),'cust_distribution':float(cust_distribution)} for tran_volume_category,cust_cnt,cust_distribution in res]
        return json.dumps(res)
    

@insights.route('/customerTransactionAmount/<int:rnk_>/<string:product_>', methods=['GET'])
def customerTransactionAmount(rnk_,product_):
    if request.method == 'GET':
        prebBaseObject()
        NBP_CD = GetSchema.Base.metadata.tables[f"nbp_customer_transaction_{rnk_}_{str.lower(product_)}"]

        
        tc =  db.session.query(func.count(func.distinct(NBP_CD.c.rim_no))).scalar()
        subquery = db.session.query(
                        NBP_CD.c.rim_no.label('rim_no'),
                        func.sum(NBP_CD.c.transaction_amount).label('tot_tran_amount')
                    ).group_by(NBP_CD.c.rim_no).subquery()

        tran_amount_category = db.case(
            [
                (subquery.c.tot_tran_amount.between(10000, 49999), 'VeryLow'),
                (subquery.c.tot_tran_amount.between(50000, 89999), 'Low'),
                (subquery.c.tot_tran_amount.between(90000, 249999), 'Medium'),
                (subquery.c.tot_tran_amount >= 250000, 'High')
            ],
            else_='VeryLow1'
        ).label('tran_amount_category')
        
        s2 = db.session.query(subquery.c.rim_no,tran_amount_category).subquery()
        
        query = db.session.query(
                s2.c.tran_amount_category,
                func.count(s2.c.tran_amount_category).label('cust_cnt'),
                func.round((func.count(s2.c.tran_amount_category) / tc) * 100, 2).label('cust_distribution')
            ).group_by(
                tran_amount_category
            )
        res = db.session.execute(query).all()
        res = [{'tran_amount_category':tran_amount_category, 'cust_cnt':float(cust_cnt),'cust_distribution':float(cust_distribution)} for tran_amount_category,cust_cnt,cust_distribution in res]
        return json.dumps(res)
    











