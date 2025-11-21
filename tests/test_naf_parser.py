from pipe_naf_reader.naf_parser import NAFParser

import csv
import pytest


def test_parse():
    inp = '//SR//AD/PAN//FR/PAN//TM/POS//NA/NESTOS REEFER//IR/46945-15//RC/3EAJ9//XR/3EAJ9//DA/190331//TI/1130//LT/+20.685//LG/-017.360//SP/0//CO/134//FS/PAN//RN/69385//ER'.split('//')
    parsed = NAFParser().parse(inp)

    assert parsed[0] == ['AD','FR','TM','NA','IR','RC','XR','DA','TI','LT','LG','SP','CO','FS','RN']
    assert parsed[1] == {'AD': 'PAN', 'FR': 'PAN', 'TM': 'POS', 'NA': 'NESTOS REEFER', 'IR': '46945-15', 'RC': '3EAJ9', 'XR': '3EAJ9', 'DA': '190331', 'TI': '1130', 'LT': '+20.685', 'LG': '-017.360', 'SP': '0', 'CO': '134', 'FS': 'PAN', 'RN': '69385'}

def test_normalize_value():
    assert NAFParser().normalize_value("a","b") == "b"

def test_process_writer_defined(monkeypatch):
    out = []

    class custom_out_writer():
        def write(self, row):
            out.append(row)

    def mock_get_header_from_schema(output_stream):
        headers = ['AD','FR','TM','NA','IR','RC','XR','DA','TI','LT','LG','SP','CO','FS','RN']
        csv_writer = csv.DictWriter(custom_out_writer(), fieldnames=headers, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writeheader()
        return csv_writer, headers

    naf = NAFParser()
    monkeypatch.setattr(naf, '_get_header_from_schema', mock_get_header_from_schema)

    inp = ["//SR//AD/PAN//FR/PAN//TM/POS//NA/NESTOS REEFER//IR/46945-15//RC/3EAJ9//XR/3EAJ9//DA/190331//TI/1130//LT/+20.685//LG/-017.360//SP/0//CO/134//FS/PAN//RN/69385//ER",
        "//SR//AD/PAN//FR/PAN//TM/POS//NA/TAI XING//IR/48319-16//RC/3EZQ5//XR/8210273//DA/190331//TI/1152//LT/+08.489//LG/+156.531//SP/70//CO/132//FS/PAN//RN/69387//ER",
        "//SR//AD/PAN//FR/PAN//TM/POS//NA/CHUNG KUO NO. 87//IR/35157-12-A//RC/HO-2539//XR/2650328//DA/190331//TI/1124//LT/+09.579//LG/-054.113//SP/70//CO/89//FS/PAN//RN/69392//ER"]

    naf.process(inp, "test")
    print(out)

    assert ''.join(out) == ("AD,FR,TM,NA,IR,RC,XR,DA,TI,LT,LG,SP,CO,FS,RN\r\n"
                   "PAN,PAN,POS,NESTOS REEFER,46945-15,3EAJ9,3EAJ9,190331,1130,+20.685,-017.360,0,134,PAN,69385\r\n"
                   "PAN,PAN,POS,TAI XING,48319-16,3EZQ5,8210273,190331,1152,+08.489,+156.531,70,132,PAN,69387\r\n"
                   "PAN,PAN,POS,CHUNG KUO NO. 87,35157-12-A,HO-2539,2650328,190331,1124,+09.579,-054.113,70,89,PAN,69392\r\n")


def test_process_no_writer(monkeypatch):
    out = []

    class custom_out_writer():
        def write(self, row):
            out.append(row)

    def mock_get_header_from_schema(output_stream):
        return None,[]

    naf = NAFParser()
    monkeypatch.setattr(naf, '_get_header_from_schema', mock_get_header_from_schema)

    inp = ["//SR//AD/PAN//FR/PAN//TM/POS//NA/NESTOS REEFER//IR/46945-15//RC/3EAJ9//XR/3EAJ9//DA/190331//TI/1130//LT/+20.685//LG/-017.360//SP/0//CO/134//FS/PAN//RN/69385//ER",
        "//SR//AD/PAN//FR/PAN//TM/POS//NA/TAI XING//IR/48319-16//RC/3EZQ5//XR/8210273//DA/190331//TI/1152//LT/+08.489//LG/+156.531//SP/70//CO/132//FS/PAN//RN/69387//ER",
        "//SR//AD/PAN//FR/PAN//TM/POS//NA/CHUNG KUO NO. 87//IR/35157-12-A//RC/HO-2539//XR/2650328//DA/190331//TI/1124//LT/+09.579//LG/-054.113//SP/70//CO/89//FS/PAN//RN/69392//ER"]

    with pytest.raises(SystemExit):
        naf.process(inp, custom_out_writer())


def test_process_writer_defined_with_expected_header(monkeypatch):
    out = []

    class custom_out_writer():
        def write(self, row):
            out.append(row)

    def mock_get_header_from_schema(output_stream):
        csv_writer = csv.DictWriter(custom_out_writer(), fieldnames=['CO','FS','RN'], delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writeheader()
        return csv_writer,['CO', 'FS', 'RN']

    naf = NAFParser()
    monkeypatch.setattr(naf, '_get_header_from_schema', mock_get_header_from_schema)

    inp = ["//SR//AD/PAN//FR/PAN//TM/POS//NA/NESTOS REEFER//IR/46945-15//RC/3EAJ9//XR/3EAJ9//DA/190331//TI/1130//LT/+20.685//LG/-017.360//SP/0//CO/134//FS/PAN//RN/69385//ER",
        "//SR//AD/PAN//FR/PAN//TM/POS//NA/TAI XING//IR/48319-16//RC/3EZQ5//XR/8210273//DA/190331//TI/1152//LT/+08.489//LG/+156.531//SP/70//CO/132//FS/PAN//RN/69387//ER",
        "//SR//AD/PAN//FR/PAN//TM/POS//NA/CHUNG KUO NO. 87//IR/35157-12-A//RC/HO-2539//XR/2650328//DA/190331//TI/1124//LT/+09.579//LG/-054.113//SP/70//CO/89//FS/PAN//RN/69392//ER"]

    naf.process(inp, "test")
    print(out)

    assert ''.join(out) == ("CO,FS,RN\r\n"
                   "134,PAN,69385\r\n"
                   "132,PAN,69387\r\n"
                   "89,PAN,69392\r\n")
