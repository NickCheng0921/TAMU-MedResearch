#%env PSQL_USER=postgres
#%env PSQL_PSWD=postgres
#%env JDBC_PATH=../TAMU-MedResearch/postgresql-42.4.0.jar
#%env PYSPARK_PYTHON=/home/ugrads/k/kingrc15/anaconda3/bin/python
import sys, time, pickle

if "/home/ugrads/n/nickcheng0921/omop-summary" not in sys.path:
    sys.path.append("/home/ugrads/n/nickcheng0921/omop-summary")
import omop_summary

sc, session = omop_summary.create_spark_context()

#load data in 8 chunks that can be tmux parallelizezd, chunks are of size 7372
chunk_num = 0
if len(sys.argv) >= 2:
    chunk_num = int(sys.argv[1])
print(f"Loading IDs: {1000+chunk_num*500} {1000+chunk_num*500+500-1}")

def tuplify(x):
    l = []
    for i in x.collect():
        l.append(tuple(i))
    return l

#cur.execute("""select hadm_id from admissions""")
list_adm_id = tuplify(omop_summary.utils.get_table("select hadm_id from mimiciii.admissions", session))

#cur.execute("select hadm_id, admission_type, trunc(extract(epoch from " +
#            "dischtime- admittime)/3600), hospital_expire_flag from admissions")
length_of_stay = omop_summary.utils.get_table("select hadm_id, admission_type, trunc(extract(epoch from dischtime - admittime)/3600), hospital_expire_flag from mimiciii.admissions", session)
#pickle.dump(tuplify(length_of_stay), open('adm_type_los_mortality.p', 'wb'))

start = time.time()
data = []
for idx in range(500): #FIXME hardcoded range
    id = 1000+chunk_num*500+idx
    end = time.time()
    #vitals are added into the list respective to list_adm_id
    
    print(f"{idx+1}/500      {id}")
    #print(id, list_adm_id[id][0])
    vitals = []

    # print("Sp02")
    v = omop_summary.utils.get_table("select charttime, valuenum from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and (itemid =" + str(646) +
                "or itemid =" + str(220277) + ")order by charttime", session)
    vitals.append(tuplify(v))

    # Heart Rate
    # print("HR")
    hr = omop_summary.utils.get_table("select charttime, valuenum from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and (itemid =" + str(211) +
                "or itemid =" + str(220045) + ")order by charttime", session)
    vitals.append(tuplify(hr))

    # Respiratory Rate
    # print("RR")
    rr = omop_summary.utils.get_table("select charttime, valuenum from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and (itemid =" + str(618) +
                "or itemid =" + str(615) + "or itemid =" + str(220210) +
                "or itemid =" + str(224690) + ")order by charttime", session)
    vitals.append(tuplify(rr))

    # Systolic Blood Pressure
    # print("SBP")
    sbp = omop_summary.utils.get_table("select charttime, valuenum from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and (itemid =" + str(51) +
                "or itemid =" + str(442) + "or itemid =" + str(455) +
                "or itemid =" + str(6701) + "or itemid =" + str(220179) +
                "or itemid =" + str(220050) + ")order by charttime", session)
    vitals.append(tuplify(sbp))

    # Diastolic Blood Pressure
    # print("DBP")
    dbp = omop_summary.utils.get_table("select charttime, valuenum from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and (itemid =" + str(8368) +
                "or itemid =" + str(8440) + "or itemid =" + str(8441) +
                "or itemid =" + str(8555) + "or itemid =" + str(220180) +
                "or itemid =" + str(220051) + ")order by charttime", session)
    vitals.append(tuplify(dbp))

    # End-tidal carbon dioxide
    # print("EtC02")
    etc02 = omop_summary.utils.get_table("select charttime, valuenum from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and (itemid =" + str(1817) +
                "or itemid =" + str(228640) + ")order by charttime", session)
    vitals.append(tuplify(etc02))

    # Temperature
    # print("Temp")
    temp = omop_summary.utils.get_table("select charttime, valuenum from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and (itemid =" + str(678) +
                "or itemid =" + str(223761) + ")order by charttime", session)
    vitals.append(tuplify(temp))

    temp2 = omop_summary.utils.get_table("select charttime, valuenum from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and (itemid =" + str(676) +
                "or itemid =" + str(223762) + ")order by charttime", session)
    vitals.append(tuplify(temp2))

    # Total Glasgow coma score
    # print("TGCS")
    tgcs = omop_summary.utils.get_table("select charttime, valuenum from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and (itemid =" + str(198) +
                "or itemid =" + str(226755) + "or itemid =" + str(227013)
                + ")order by charttime", session)
    vitals.append(tuplify(tgcs))

    # Peripheral capillary refill rate
    # print("CRR")
    crr = omop_summary.utils.get_table("select charttime, value from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and itemid =" + str(3348) +
                "order by charttime", session)
    vitals.append(tuplify(crr))
    crr2 = omop_summary.utils.get_table("select charttime, value from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and (itemid =" + str(115) +
                "or itemid = 223951) order by charttime", session)
    vitals.append(tuplify(crr2))
    crr3 = omop_summary.utils.get_table("select charttime, value from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and (itemid =" + str(8377) +
                "or itemid = 224308) order by charttime", session)
    vitals.append(tuplify(crr3))

    # Urine output 43647, 43053, 43171, 43173, 43333, 43347, 43348, 43355, 43365, 43373, 43374, 43379
    # print("UO")
    uo = omop_summary.utils.get_table("select charttime, VALUE from mimiciii.outputevents where hadm_id ="
                + str(list_adm_id[id][0]) + " and ( itemid = 40405 or itemid =" +
                " 40428 or itemid = 41857 or itemid = 42001 or itemid = 42362 or itemid =" +
                " 42676 or itemid = 43171 or itemid = 43173 or itemid = 42042 or itemid =" +
                " 42068 or itemid = 42111 or itemid = 42119 or itemid = 40715 or itemid =" +
                " 40056 or itemid = 40061 or itemid = 40085 or itemid = 40094 or itemid =" +
                " 40096 or itemid = 43897 or itemid = 43931 or itemid = 43966 or itemid =" +
                " 44080 or itemid = 44103 or itemid = 44132 or itemid = 44237 or itemid =" +
                " 43348 or itemid =" +
                " 43355 or itemid = 43365 or itemid = 43372 or itemid = 43373 or itemid =" +
                " 43374 or itemid = 43379 or itemid = 43380 or itemid = 43431 or itemid =" +
                " 43462 or itemid = 43522 or itemid = 44706 or itemid = 44911 or itemid =" +
                " 44925 or itemid = 42810 or itemid = 42859 or itemid = 43093 or itemid =" +
                " 44325 or itemid = 44506 or itemid = 43856 or itemid = 45304 or itemid =" +
                " 46532 or itemid = 46578 or itemid = 46658 or itemid = 46748 or itemid =" +
                " 40651 or itemid = 40055 or itemid = 40057 or itemid = 40065 or itemid =" +
                " 40069 or itemid = 44752 or itemid = 44824 or itemid = 44837 or itemid =" +
                " 43576 or itemid = 43589 or itemid = 43633 or itemid = 43811 or itemid =" +
                " 43812 or itemid = 46177 or itemid = 46727 or itemid = 46804 or itemid =" +
                " 43987 or itemid = 44051 or itemid = 44253 or itemid = 44278 or itemid =" +
                " 46180 or itemid = 45804 or itemid = 45841 or itemid = 45927 or itemid =" +
                " 42592 or itemid = 42666 or itemid = 42765 or itemid = 42892 or itemid =" +
                " 43053 or itemid = 43057 or itemid = 42130 or itemid = 41922 or itemid =" +
                " 40473 or itemid = 43333 or itemid = 43347 or itemid = 44684 or itemid =" +
                " 44834 or itemid = 43638 or itemid = 43654 or itemid = 43519 or itemid =" +
                " 43537 or itemid = 42366 or itemid = 45991 or itemid = 43583 or itemid =" +
                " 43647) order by charttime ", session)
    vitals.append(tuplify(uo))

    # Fraction inspired oxygen
    # print("Fi02")
    fi02 = omop_summary.utils.get_table("select charttime, valuenum from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and (itemid =" + str(2981) +
                "or itemid =" + str(3420) + "or itemid =" + str(3422) +
                "or itemid =" + str(223835) + ")order by charttime", session)
    vitals.append(tuplify(fi02))

    # Glucose 807,811,1529,3745,3744,225664,220621,226537
    # print("Glucose")
    gluc = omop_summary.utils.get_table("select charttime, valuenum from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and (itemid =" + str(807) +
                "or itemid =" + str(811) + "or itemid =" + str(1529) +
                "or itemid =" + str(3745) + "or itemid =" + str(3744) +
                "or itemid =" + str(225664) + "or itemid =" + str(220621) +
                "or itemid =" + str(226537) + ")order by charttime", session)
    vitals.append(tuplify(gluc))

    # pH 780, 860, 1126, 1673, 3839, 4202, 4753, 6003, 220274, 220734, 223830, 228243,
    # print("pH")
    ph = omop_summary.utils.get_table("select charttime, valuenum from mimiciii.chartevents where hadm_id ="
                + str(list_adm_id[id][0]) + "and (itemid =" + str(780) +
                "or itemid =" + str(860) + "or itemid =" + str(1126) +
                "or itemid =" + str(1673) + "or itemid =" + str(3839) +
                "or itemid =" + str(4202) + "or itemid =" + str(4753) +
                "or itemid =" + str(6003) + "and itemid =" + str(220274) +
                "or itemid =" + str(220734) + "or itemid =" + str(223830) +
                "or itemid =" + str(228243) + ") order by charttime", session)
    vitals.append(tuplify(ph))

    data.append(vitals)

final_name = 'vitals_records_' + str(1000+chunk_num*500) + '_' + str(1000+chunk_num*500+500-1) + '.p' #FIXME hardcoded file name
pickle.dump(data, open(final_name, 'wb'))
end = time.time()
print(f"Total: {1.0*(end-start)/60} minutes")