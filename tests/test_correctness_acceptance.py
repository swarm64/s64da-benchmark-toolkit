
import pytest
import pandas

from io import StringIO

from s64da_benchmark_toolkit.correctness import Correctness


# Acceptance test for correctness checking

CSV_BASE = '''cnt,state,value
20,HI,1.0
45,RI,100000000000000000.0
59,CT,123
914,IA,9999999999.131413000000000000000000000000001
101,NH,451
120,,
988,IL,0.5
290,NM,0.000000000000000000000000000000000001
29,DE,0.00000001
986,,42
,MO,NaN
1055,KS,
2420,TX,1024
-1,FOO,9999999999.139413000000000000000000000000001'''

CSV_ERRORS_IDX = [3, 4, 8] # after sorting
CSV_ERRORS = '''cnt,state,value
20,HI,1.0
45,RI,
101,NH,451
120,,
59,CT,123
988,IL,0.5
,NM,0.000000000000000000000000000000000001
914,IA,9999999999.131413000000000000000000000000001
29,,0.00000001
986,,42
,MO,
1055,KS,
2420,TX,1024
-1,FOO,9999999999.139413000000000000000000000000001'''

CSV_ERRORS_PRECISION_IDX = [9]
CSV_ERRORS_PRECISION = '''cnt,state,value
20,HI,1.0
45,RI,100000000000000000.0
59,CT,123
914,IA,9999999999.149413000000000000000000000000001
101,NH,451
120,,
988,IL,0.5
290,NM,0.000000000000000000000000000000000001
29,DE,0.00000001
986,,42
,MO,
1055,KS,
2420,TX,1024
-1,FOO,9999999999.139413000000000000000000000000001'''

CSV_WITHIN_PRECISION = '''cnt,state,value
20,HI,1.0
45,RI,100000000000000000.0
59,CT,123
914,IA,9999999999.131413000000000000000000000000001
101,NH,451
120,,
988,IL,0.5
290,NM,0.000000000000000000000000000000000001
29,DE,0.00000001
986,,42
,MO,
1055,KS,
2420,TX,1024
-1,FOO,9999999999.141413000000000000000000000000001'''


ROWS_TO_DROP = [6, 8]
COLUMNS_TO_DROP = ['state']


@pytest.fixture()
def correctness():
    benchmark = 'tpch'
    scale_factor = 1000

    return Correctness(scale_factor, benchmark)


def get_dataframe(source):
    source_io = StringIO(source)
    return pandas.read_csv(source_io, sep=',')


def test_correctness_truth_empty(correctness):
    truth = pandas.DataFrame()
    result = get_dataframe(CSV_BASE)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == list(result.index)


def test_correctness_result_empty(correctness):
    truth = get_dataframe(CSV_BASE)
    result = pandas.DataFrame()

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == list(truth.index)


def test_correctness_full_equal(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_BASE)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == []


def test_correctness_full_equal_column_swapped(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_BASE).reindex(columns=['state', 'cnt', 'value'])

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == []


def test_correctness_precision(correctness):
    CSV_PRECISION_A = '''id,value
    1,9999999999.1349
    2,9999999999.1399'''
    truth = get_dataframe(CSV_PRECISION_A)

    CSV_PRECISION_B = '''id,value
    1,9999999999.1449
    2,9999999999.1448'''
    result = get_dataframe(CSV_PRECISION_B)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == [0]


def test_tpch_precision(correctness):
    truth = pandas.DataFrame([[0.05000012926042882], [0.04999679231408742]])
    result = pandas.DataFrame([[0.0499967923141588], [0.0500001292604431]])

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == []


def test_correctness_full_equal_within_precision(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_WITHIN_PRECISION)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == []


def test_correctness_full_equal_randomized_rows(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_BASE).sample(frac=1, random_state=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == []


def test_correctness_full_equal_randomized_columns(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_BASE).sample(frac=1, random_state=1, axis=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == []


def test_correctness_full_equal_randomized_full(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_BASE) \
        .sample(frac=1, random_state=1) \
        .sample(frac=1, random_state=1, axis=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == []


def test_correctness_not_equal_row_missing_truth(correctness):
    truth = get_dataframe(CSV_BASE).drop(ROWS_TO_DROP)
    result = get_dataframe(CSV_BASE)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == list(truth.index)


def test_correctness_not_equal_row_missing_result(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_BASE).drop(ROWS_TO_DROP)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == list(truth.index)


def test_correctness_not_equal_column_missing_truth(correctness):
    truth = get_dataframe(CSV_BASE).drop(COLUMNS_TO_DROP, axis=1)
    result = get_dataframe(CSV_BASE)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == list(truth.index)


def test_correctness_not_equal_column_missing_result(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_BASE).drop(COLUMNS_TO_DROP, axis=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == list(truth.index)


def test_correctness_not_equal(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_ERRORS)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == CSV_ERRORS_IDX


def test_correctness_not_equal_randomized_rows(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_ERRORS).sample(frac=1, random_state=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == CSV_ERRORS_IDX


def test_correctness_not_equal_randomized_columns(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_ERRORS).sample(frac=1, random_state=1, axis=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == CSV_ERRORS_IDX


def test_correctness_not_equal_randomized_full(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_ERRORS) \
        .sample(frac=1, random_state=1) \
        .sample(frac=1, random_state=1, axis=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == CSV_ERRORS_IDX


def test_correctness_not_equal_precision(correctness):
    truth = get_dataframe(CSV_BASE)
    result = get_dataframe(CSV_ERRORS_PRECISION)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == CSV_ERRORS_PRECISION_IDX


def test_value_swap_rows(correctness):
    truth = pandas.DataFrame([[1, 2], [4, 5]])
    result = pandas.DataFrame([[2, 1], [4, 5]])

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == [0]


def test_value_swap_columns(correctness):
    truth = pandas.DataFrame([[1, 4], [2, 5]])
    result = pandas.DataFrame([[2, 4], [1, 5]])

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == [0, 1]


def test_correctness_full_equal_text_shuffled(correctness):
    CSV_TEXT = '''s_store_name,i_item_desc,revenue,i_current_price,i_wholesale_cost,i_brand
    able,A,5.93,8.61,3.61,exportiedu pack #2
    able,A,5.44,5.33,3.09,edu packimporto #1
    able,A,8.07,2.97,1.18,edu packamalg #2
    able,A,5.10,2.00,1.24,edu packunivamalg #9
    able,A,4.70,3.36,1.41,amalgunivamalg #8
    able,A,1.55,5.86,5.21,corpmaxi #11
    able,A,1.81,5.86,5.21,corpmaxi #11
    able,A,5.53,0.58,0.17,importounivamalg #5
    able,A,1.00,8.69,4.17,edu packexporti #1
    able,"A bit constant books unite human phenomena. Other offences ought to challenge traditions. Forces work downstairs in a workers. Urgent comments may draw arguably critical, busy ",1.44,4.78,2.58,importocorp #4
    able,"A bit excellent pages help human households. Rare, happy eyes avoid instruments. P",3.69,3.36,2.55,amalgamalg #2
    able,"A bit new books tell supporters. Institutional, small dimensions ",1.80,2.96,2.07,exportiunivamalg #3
    able,A bit other groups bring suddenly usual services. Details work at first italian hands. Certain issues would form pp.; free drawings must allow independ,0.16,3.73,2.31,importounivamalg #9
    able,A bit popular commentators will not beg most great words. So wrong problems ought to punch also. Good citizens equate tight succe,3.56,1.21,0.78,importounivamalg #11
    able,A bit scottish obligations ought to feel almost local difficulties. Acceptable symptoms assign skills; convincing groups hel,4.88,1.99,1.59,amalgmaxi #9
    able,A bit top candidates see constantly corporate options. Here true ,0.81,1.63,1.09,exportiimporto #1
    able,"A little brown parties could wait councils. Yellow, private decisi",2.31,3.82,2.33,edu packedu pack #1
    able,"A little confident sales might handle women. Only traditional forms begin heavily well good features. Consistent, able languages let away able goods. Confidential objects cannot support police. ",4.65,9.86,7.98,importounivamalg #2
    able,"A little parental odds sign parliamentary councils. Important, real units could grieve now civil proposals; years might not matter then social groups. Joint, free schemes use widely palestini",7.39,8.25,5.69,exportiexporti #1
    able,A little political arguments trust t,8.61,3.74,2.28,scholarcorp #7
    able,A little political arguments trust t,3.77,3.74,2.28,scholarcorp #7
    able,A little political police could specify radical developments; dates could hear to the ministers; almost national symptoms hold however on the products. Organic compu,7.49,46.33,34.28,importounivamalg #13
    able,A little political police could specify radical developments; dates could hear to the ministers; almost national symptoms hold however on the products. Organic compu,6.01,46.33,34.28,importounivamalg #13
    able,"A little strong arts solve particularly. Circles go interests. Sheets prove ago forms. Letters must minimise. Rare, only women offer then organisms. Inner children must not continue theref",1.32,4.32,2.41,exportiunivamalg #4
    able,A little true companies answer; real events carry. Languages c,0.57,2.86,1.22,univmaxi #7
    able,Aback black readers form just at ,4.43,4.50,1.39,importoexporti #1
    able,Aback historic days could not remain by a traditions. Correspond,4.01,2.46,0.86,amalgexporti #2
    able,Able areas used to take ago unknown forest,1.03,1.49,1.04,amalgbrand #3
    able,"Able authorities can think extra, local changes. Proceedings mark hard men. Particular, statutory practitioners play then adu",7.34,0.15,0.08,scholarbrand #3
    able,Able authors work near however dif,2.20,7.61,5.70,namelessbrand #5
    able,"Able babies maintain approximately old instances. Blue words ought to kill current, natural years; exact, prime discussions would not t",4.44,2.90,2.37,exportiimporto #2
    able,"Able casualties apply as places. Alternatives go then just individual issues. Old, expert demands must not see highly hours; willing, liberal children would not remember regional, unable bird",7.60,4.02,1.28,amalgexporti #1
    able,"Able children upset. Appropriately important women will stay on the discussions; skilled, royal levels might regret most more available effects. Old years commence well wooden victims. Speakers wo",3.48,0.38,0.26,scholarbrand #1
    able,Able circumstances stay far young rights. Too fat parties may ensure rights. European police make aware researchers; val,2.48,8.62,5.60,amalgscholar #2
    able,"Able companies admit at least minimum schools. Also sensitive opportunities provide. Maximum nurses attempt central, human areas; quick examples take red, serious",3.05,19.47,15.57,exportiunivamalg #13
    able,"Able companies please to a procedures. Children create then either political bonds; other pupils let even vast, left hands. Other armies will prom",7.59,9.93,3.87,amalgamalg #1
    able,"Able companies please to a procedures. Children create then either political bonds; other pupils let even vast, left hands. Other armies will prom",2.30,9.93,3.87,amalgamalg #1
    able,"Able costs turn no longer. Heavy bodies use because of the degrees. Traditional, medical cars act then animals. Attempts could not sue twice better cultural weaknesses. High lives can reach russians. ",1.78,69.37,61.04,maxinameless #3
    able,Able courses could go as free results; rebels defend even with a years. New policies pick yesterday widely free processes. Early hands mind both eyes. Farmers ought to stand pr,1.52,94.07,73.37,edu packimporto #1
    able,"Able doubts fight painfully. Corporate, isolated shoes get too products. Practica",1.27,1.98,1.20,univbrand #5
    able,"Able doubts fight painfully. Corporate, isolated shoes get too products. Practica",0.91,1.98,1.20,univbrand #5
    able,"Able eyes guide wrong lives. Factors leave public, free pictures. I",3.11,0.88,0.29,edu packedu pack #1
    able,"Able factories must hold prime, black c",0.79,6.28,4.64,edu packamalg #1
    able,"Able factories must hold prime, black c",0.88,6.28,4.64,edu packamalg #1
    able,Able friends af,5.33,7.21,5.33,importoimporto #1
    able,"Able friends shall respond all right happy details; too sensitive investments could not make however great, correct reasons; big, ruling servants look at least. R",8.82,1.20,0.76,corpbrand #8
    able,"Able incidents make. Rapid, increased issues face yet big feelings. Similar sales signal then brilliant troops. Farmers repair. Again due neighbours lif",4.49,1.38,0.85,importoamalg #1
    able,"Able incidents make. Rapid, increased issues face yet big feelings. Similar sales signal then brilliant troops. Farmers repair. Again due neighbours lif",5.21,1.38,0.85,importoamalg #1
    able,"Able investigations flourish also social options. Bodies appear. Black, tiny services might not wait more estimates. So internal problems work yesterday regular terms. Ago usef",4.76,4.67,4.01,exportibrand #9
    able,"Able lives shall organize here national clothes. Now other patients shall admit only nice payments. Symptoms talk in order long forces. Good, wrong sides give in addition alternative, valuable patien",3.97,8.31,3.65,edu packimporto #1
    able,"Able lives shall organize here national clothes. Now other patients shall admit only nice payments. Symptoms talk in order long forces. Good, wrong sides give in addition alternative, valuable patien",3.85,8.31,3.65,edu packimporto #1
    able,Able members ought to write however. Governments could not progress carers. Physically short questions cannot compensate right in a schoo,7.43,3.40,2.07,amalgimporto #2
    able,"Able negotiations might protect so months. Contemporary, sharp estimates shall burn accordingly to a movements. Cen",5.65,8.09,3.47,edu packscholar #1
    able,"Able operations rebuild certain, dual things. Policies will switch bril",3.49,0.79,0.63,exportiexporti #2
    able,Able opponents ought to stimulate in,2.26,3.12,1.18,univbrand #1
    able,Able opponents ought to stimulate in,4.07,3.12,1.18,univbrand #1
    able,Able opponents ought to stimulate in,3.67,3.12,1.18,univbrand #1
    able,"Able policies shall not tell just main, brief forces. High poles cannot pick here over closed institutions. Programs can repay about ill chief bodies. Too straightforwa",0.00,3.17,1.90,edu packamalgamalg #11
    able,"Able products leave of course; single, economic streets round with a actions. Nearly true cameras can argue chairs. Thanks clear reasonably in a conditi",7.61,0.35,0.28,edu packamalg #1
    able,Able reactions stay rigidly ministers;,5.62,2.97,2.13,namelessunivamalg #12
    able,Able reactions stay rigidly ministers;,3.09,2.97,2.13,namelessunivamalg #12
    able,"Able results used to enjoy royal, severe games. Frantically political scientists trip",0.16,1.65,1.23,namelessbrand #8
    able,"Able results used to enjoy royal, severe games. Frantically political scientists trip",1.21,1.65,1.23,namelessbrand #8
    able,Able scientists shall no,2.25,4.12,1.52,amalgscholar #2
    able,Able scientists shall no,4.20,4.12,1.52,amalgscholar #2
    able,Able scientists shall no,4.39,4.12,1.52,amalgscholar #2
    able,"Able trains bend also now able states. Brown, critical years might attend. Meetings suggest else other members",0.51,6.71,3.15,amalgbrand #8
    able,Able years intrude terribly left years.,5.16,25.41,20.07,importoamalg #2
    able,"Able, able allegations contact yet tories. Small, healthy experiences give players. Still new records would not subscribe ultimat",5.85,4.72,4.24,scholarunivamalg #6
    able,"Able, adult views suggest relatively unpleasant ",8.43,3.97,1.19,importonameless #1
    able,"Able, complete messages deal concerned, single factors. So sure worlds contribute police. Main masters will not become in a eyes. Countries see never yet left beliefs. Complaints would expect ne",7.59,7.64,4.81,exportiamalgamalg #14
    able,"Able, cultural countries should not mean other, big meetings. Parents take in a pressures. Exactly likely quantities might not refuse particular plans. Central g",4.86,0.38,0.18,edu packimporto #1
    able,"Able, far years leave seriously projects. Available, practical eyes say likely, conservative women. Necessary",6.86,1.33,0.98,importoamalg #2
    able,"Able, french cars might see private aspects. Outside, rural methods see high ill things; both considerable regulations may not get. Then powerful points ",0.00,5.90,2.24,amalgscholar #1
    able,"Able, general pictures g",6.37,8.25,6.43,univnameless #4
    able,"Able, high teachers will not insist never both given measures. Alone, sick options increase there. Fond, primary examples shall not keep likely, new supporters. Troops shall not ",8.21,1.82,1.21,exportiunivamalg #8
    able,"Able, historical affairs attribu",0.17,2.98,1.43,exportiimporto #1
    able,"Able, hot patterns make so confidential meals. Even easy photographs get radical parts. Specialists can feel for example english, elderly insects. True, young buildings hear so powerful savings. Soci",3.08,3.25,2.56,namelessbrand #4
    able,"Able, important cats",4.96,7.08,2.54,edu packimporto #1
    able,"Able, inc details confer european lights. Habits provide high notes. Odd men give furthermore often magnetic resour",1.75,1.76,1.14,amalgedu pack #2
    able,"Able, inc details confer european lights. Habits provide high notes. Odd men give furthermore often magnetic resour",6.08,1.76,1.14,amalgedu pack #2
    able,"Able, other years like rather huge methods. Years sh",5.21,3.95,2.56,importoedu pack #2
    able,"Able, outside ways could stay. Commitments come local things. Present circumstances let shy, other vehicles. Geographical, significant eyes ought to take over practical, new opinions. Teachers shou",4.37,2.55,1.12,brandmaxi #5
    able,"Able, professional lines discern respectively regular eyes. Full babies change. Parties would not go still on the relationships; occupatio",4.46,5.81,2.55,exporticorp #3
    able,"Able, sure provisions climb",6.19,0.45,0.31,edu packamalg #1
    able,"Able, technological figures need. Often silent requirements might need as catholic, numerous readers. Strategies would listen rather just different courses.",3.55,1.00,0.68,amalgexporti #1
    able,"Able, true questions would make with a l",7.53,2.13,1.57,importobrand #3
    able,"Able, young leaders offer so. Sudden, english years may claim even efficient conditions. Yet theoretical records see remarkable, social circles. Tall, real grounds want at a things. Factories ",2.22,2.40,1.48,exportiimporto #1
    able,Abou,8.77,6.61,2.04,importoamalg #2
    able,About available needs find scientists. Modern arguments would allow to the day,0.72,3.89,1.75,scholaramalgamalg #11
    able,About blind knees guess by a lists. Aware minutes point only. Tools stay also possibly trying funds. Visitors will no,2.71,2.86,1.91,scholarbrand #9
    able,About bright stages can constitute far matters; diffe,5.76,7.88,4.49,namelessbrand #5
    able,"About crucial daughters may blame for the results. About private hands would not consider least sufficient, permanent stands. Then objective sign",1.50,2.30,1.49,edu packscholar #1
    able,About difficult applicat,6.29,5.60,4.36,exportischolar #1
    able,About elderly centuries could ask really a,6.97,0.14,0.04,scholarbrand #8
    able,About foreign arguments might not put all anyway imposs,7.86,1.38,1.02,amalgimporto #2
    able,About frie,4.37,5.87,3.99,edu packexporti #2
    able,"About good rates might not consider forward for a texts. English, genetic rights can make. Clear, additional police would not provide thick workers. Qu",1.10,1.04,0.48,brandmaxi #11
    able,"About good rates might not consider forward for a texts. English, genetic rights can make. Clear, additional police would not provide thick workers. Qu",3.92,1.04,0.48,brandmaxi #11
    able,About great boys will not eliminate too,1.15,5.22,3.75,amalgimporto #1'''

    truth = get_dataframe(CSV_TEXT)
    result = get_dataframe(CSV_TEXT).sample(frac=1, random_state=1)

    mismatch_idx = correctness._check_correctness_impl(truth, result)
    assert mismatch_idx == []
