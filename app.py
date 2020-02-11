import pandas as pd
from psycopg2 import connect
import sqlalchemy as sa
from pymorphy2 import MorphAnalyzer
from json import load, dumps, loads
import re
from functools import reduce
from itertools import product
from multiprocessing import Pool, Lock
import logging as log
from flask import Flask, request


app = Flask(__name__)
TEST_MODE = False

log.basicConfig(
    format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', level=log.DEBUG)
morph = MorphAnalyzer()
in_ = 'abcekxhtpomё'
out = 'авсекхнтроме'
translation = str.maketrans(in_, out)
db = None

with open('anchors.json', encoding='utf-8') as f:
    anchors = load(f)


def get_bi_gramm(rec):
    if len(rec.formalname.split()) < 2:
        rec.full1 = f'{rec.formalname.lower()} {rec.shortname.lower()}'
        rec.full2 = f'{rec.shortname.lower()} {rec.formalname.lower()}'
    else:
        rec.full1 = f'{"_".join(rec.formalname.lower().split())}_{rec.shortname.lower()}'
        rec.full2 = f'{rec.shortname.lower()}_{"_".join(rec.formalname.lower().split())}'
    return rec


def clear(text):
    text_ = []
    text = text.lower().translate(translation)
    text = re.sub(r'(^|\W)с(\W|$)', ' ', text)
    text = re.sub(r'[^\w\d -]', ' ', text)
    while '  ' in text:
        text = text.replace('  ', ' ')
    for to_, from_ in anchors.items():
        text = re.sub(from_, to_, text)
    for word in text.split():
        flag = False
        parsed = list(filter(lambda x: ('NOUN' in x.tag or 'ADJF' in x.tag or 'PREP' in x.tag or 'UNKN' in x.tag or 'PRTF' in x.tag) and len(
            x.word) > 2, morph.parse(word)))
        if parsed:
            flag = True
            text_.append(reduce(lambda a, x: a.union(
                set(map(lambda x: x.word, x.lexeme))), parsed, set()))
        for to_, from_ in anchors.items():
            word = re.sub(from_, to_, word)
        if flag:
            text_[-1].add(word)
        else:
            text_.append({word})

    new = []
    for s in range(len(text_) - 1):
        new += list(map(lambda x: f'{x[0]} {x[1]}',
                        product(text_[s], text_[s+1])))
    for s in range(len(text_) - 2):
        new += list(map(lambda x: f'{x[0]}_{x[1]}_{x[2]}',
                        product(text_[s], text_[s+1], text_[s+2])))
    return pd.DataFrame(new)


def main(rec, db):
    descr = rec.descr
    data = []
    sflag, mflag, cflag = False, False, False
    try:
        res = clear(descr)
    except Exception as e:
        log.error(e)
        rec = dict(rec)
        rec['data'] = data
        return rec
    try:
        merged = db.merge(res, left_on='full1', right_on=0).append(
            db.merge(res, left_on='full2', right_on=0), ignore_index=True).drop_duplicates(subset='aoguid')
    except Exception as e:
        log.error(e)
        rec = dict(rec)
        rec['data'] = data
        return rec
    subjects = merged[merged.aolevel == 1]
    sstatus = 'found'
    if subjects.empty:
        sstatus = 'predefined'
        subjects = db[db.aolevel == 1][db.full1 == rec.subject.lower()]
    for s in subjects.itertuples():
        restore = False
        mo = merged[merged.parentguid == s.aoguid][merged.aolevel == 3]
        mostatus = 'found'
        if mo.empty:
            mo = db[db.aolevel == 3][db.full1 == rec.mo.lower()]
            mostatus = 'predefined'
        if mo.empty:
            restore = True
            mo = [s]
            mostatus
        else:
            mo = mo.itertuples()

        for m in mo:
            mflag = True
            cities = merged[3 < merged.aolevel][merged.aolevel < 7][(
                merged.subj if restore else merged.parentguid) == m.aoguid]
            cities = cities[cities.parentguid == m.aoguid]
            if cities.empty:
                restore = True
            cstatus = 'found'
            if cities.empty:
                cstatus = 'predefined'
                cities = db[db.aolevel.isin(
                    (4, 5, 6))][db.full1 == rec.city.lower()]
            for c in cities.itertuples():
                cflag = True
                # Восстановление МО
                if restore:
                    m = db[db.aoguid == c.parentguid][db.aolevel == 3]
                    if m.empty:
                        m = c
                    else:
                        m = m.iloc[0]
                    mostatus = 'restored'

                streets = merged[merged.aolevel ==
                                 7][merged.parentguid == c.aoguid]
                for st in streets.itertuples():
                    sflag = True
                    data.append({'subject': ' '.join((s.formalname, s.shortname)), 'sstatus': sstatus, 'mo': ' '.join((m.formalname, m.shortname)), 'mostatus': mostatus,
                                 'city': ' '.join((c.formalname, c.shortname)), 'cstatus': cstatus, 'street': ' '.join((st.formalname, st.shortname)), 'ststatus': 'found'})
                    data[-1]['full'] = ', '.join(
                        (data[-1]['subject'], data[-1]['mo'], data[-1]['city'], data[-1]['street']))

        ### Запись результатов ###

                if not sflag and 'found' in (sstatus, mostatus, cstatus):
                    data.append({'subject': ' '.join((s.formalname, s.shortname)), 'sstatus': sstatus, 'mo': ' '.join(
                        (m.formalname, m.shortname)), 'mostatus': mostatus, 'city': ' '.join((c.formalname, c.shortname)), 'cstatus': cstatus})
                    data[-1]['full'] = ', '.join((data[-1]
                                                  ['subject'], data[-1]['mo'], data[-1]['city']))
            if not cflag and 'found' in (sstatus, mostatus):
                data.append({'subject': ' '.join((s.formalname, s.shortname)), 'sstatus': sstatus, 'mo': ' '.join(
                    (m.formalname, m.shortname)), 'mostatus': mostatus})
                data[-1]['full'] = ', '.join((data[-1]
                                              ['subject'], data[-1]['mo']))
        if not mflag and 'found' == sstatus:
            data.append({'subject': ' '.join(
                (s.formalname, s.shortname)), 'sstatus': sstatus})
            data[-1]['full'] = data[-1]['subject']
        ##########################
    for i in range(len(data)):
        data[i]['full'] = reduce(lambda a, x: a + ', ' +
                                 x if not x in a else a, data[i]['full'].split(', '))
    rec = dict(rec)
    rec['data'] = data
    return rec


def process(test, i, db):
    log.debug(f'Process {i} started!')

    c, l = 1, len(test)
    for _, t in test.iterrows():
        try:
            res = main(t, db)
            lock.acquire()
            with open('temp_res.txt', 'a', encoding='utf-8') as f:
                f.write(dumps(res, ensure_ascii=False) + '\n')
            lock.release()
        finally:
            log.debug(f'Process {i}: {c}/{l}')
            c += 1


@app.route('/parse', methods=['GET', 'POST'])
def flas():
    global db
    req = request.json
    df = pd.read_json(dumps(req), orient='records')
    res = []
    for _, t in df.iterrows():
        try:
            res.append(main(t, db))
        except Exception as e:
            if TEST_MODE:
                log.exception(e)
    return dumps(res, ensure_ascii=False)


def init(l):
    global lock
    lock = l


if __name__ == "__main__":
    lock = Lock()
    # log.info('Reading xlsx...')
    # test = pd.read_excel('тест_поиск_адреса.xlsx').fillna('')
    engine = sa.create_engine(
        "postgresql+psycopg2://rwayweb:rwayweb@10.199.13.62/rwayweb")
    log.info('Downloading db...')
    db = pd.read_sql_query(f'''SELECT 
        aoguid,
        parentguid,
        aolevel,
        formalname,
        shortname
    FROM fias.addrobjects
    WHERE aolevel <= 7 and actstatus = 1 {"LIMIT 10000" if TEST_MODE else ""}''', engine)
    log.info('DB loaded')
    log.info('Creating bi-gramms...')
    # Предобработка фиаса
    db['full1'] = [None] * len(db)
    db['full2'] = [None] * len(db)
    db['subj'] = [None] * len(db)
    db = db.apply(get_bi_gramm, axis=1)
    log.info('Making parent subject...')
    subjects = db[db.aolevel == 1]
    c, l = 1, len(subjects)
    for rec in subjects.itertuples():
        next = db[db.parentguid == rec.aoguid]
        while not next.empty:
            db.loc[next.index, 'subj'] = rec.aoguid
            next = db[db.parentguid.isin(next.aoguid)]
        log.debug(f'{c}/{l}\t{rec.formalname} done')
        c += 1
    app.run('10.199.13.111', 9516) if not TEST_MODE else app.run()
