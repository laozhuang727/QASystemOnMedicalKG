#!/usr/bin/env python3
# coding: utf-8
# File: MedicalGraph.py
# Author: lhy<lhy_in_blcu@126.com,https://huangyong.github.io>
# Date: 18-10-3

import os
import json
from py2neo import Graph, Node
# import sys
# reload(sys)
# sys.setdefaultencoding('utf8')

import pandas as pd
import csv


class MedicalGraph:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.data_path = os.path.join(cur_dir, 'data/medical.json')

    '''读取文件'''

    def read_nodes(self):
        # 共７类节点
        drug_jsons = []  # 药品
        food_jsons = []  # 食物
        check_jsons = []  # 检查
        department_jsons = []  # 科室
        producer_jsons = []  # 药品大类
        disease_jsons = []  # 疾病
        symptom_jsons = []  # 症状

        disease_infos = []  # 疾病信息

        # 构建节点实体关系
        rels_department = []  # 科室－科室关系
        rels_noteat = []  # 疾病－忌吃食物关系
        rels_doeat = []  # 疾病－宜吃食物关系
        rels_recommandeat = []  # 疾病－推荐吃食物关系
        rels_commonddrug = []  # 疾病－通用药品关系
        rels_recommanddrug = []  # 疾病－热门药品关系
        rels_check = []  # 疾病－检查关系
        rels_drug_producer = []  # 厂商－药物关系

        rels_symptom = []  # 疾病症状关系
        rels_acompany = []  # 疾病并发关系
        rels_category = []  # 疾病与科室之间的关系

        count = 0
        for data in open(self.data_path, encoding="utf-8"):
            disease_dict = {}
            count += 1
            print(count)
            data_obj = json.loads(data)
            disease = data_obj['name']
            disease_dict['name'] = disease
            disease_jsons.append(disease)
            disease_dict['desc'] = ''
            disease_dict['prevent'] = ''
            disease_dict['cause'] = ''
            disease_dict['easy_get'] = ''
            disease_dict['cure_department'] = ''
            disease_dict['cure_way'] = ''
            disease_dict['cure_lasttime'] = ''
            disease_dict['symptom'] = ''
            disease_dict['cured_prob'] = ''

            if 'symptom' in data_obj:
                symptom_jsons += data_obj['symptom']
                for symptom in data_obj['symptom']:
                    rels_symptom.append([disease, symptom])

            if 'acompany' in data_obj:
                for acompany in data_obj['acompany']:
                    rels_acompany.append([disease, acompany])

            if 'desc' in data_obj:
                disease_dict['desc'] = data_obj['desc']

            if 'prevent' in data_obj:
                disease_dict['prevent'] = data_obj['prevent']

            if 'cause' in data_obj:
                disease_dict['cause'] = data_obj['cause']

            if 'get_prob' in data_obj:
                disease_dict['get_prob'] = data_obj['get_prob']

            if 'easy_get' in data_obj:
                disease_dict['easy_get'] = data_obj['easy_get']

            if 'cure_department' in data_obj:
                cure_department = data_obj['cure_department']
                if len(cure_department) == 1:
                    rels_category.append([disease, cure_department[0]])
                if len(cure_department) == 2:
                    big = cure_department[0]
                    small = cure_department[1]
                    rels_department.append([small, big])
                    rels_category.append([disease, small])

                disease_dict['cure_department'] = cure_department
                department_jsons += cure_department

            if 'cure_way' in data_obj:
                disease_dict['cure_way'] = data_obj['cure_way']

            if 'cure_lasttime' in data_obj:
                disease_dict['cure_lasttime'] = data_obj['cure_lasttime']

            if 'cured_prob' in data_obj:
                disease_dict['cured_prob'] = data_obj['cured_prob']

            if 'common_drug' in data_obj:
                common_drug = data_obj['common_drug']
                for drug in common_drug:
                    rels_commonddrug.append([disease, drug])
                drug_jsons += common_drug

            if 'recommand_drug' in data_obj:
                recommand_drug = data_obj['recommand_drug']
                drug_jsons += recommand_drug
                for drug in recommand_drug:
                    rels_recommanddrug.append([disease, drug])

            if 'not_eat' in data_obj:
                not_eat = data_obj['not_eat']
                for _not in not_eat:
                    rels_noteat.append([disease, _not])

                food_jsons += not_eat
                do_eat = data_obj['do_eat']
                for _do in do_eat:
                    rels_doeat.append([disease, _do])

                food_jsons += do_eat
                recommand_eat = data_obj['recommand_eat']

                for _recommand in recommand_eat:
                    rels_recommandeat.append([disease, _recommand])
                food_jsons += recommand_eat

            if 'check' in data_obj:
                check = data_obj['check']
                for _check in check:
                    rels_check.append([disease, _check])
                check_jsons += check
            if 'drug_detail' in data_obj:
                drug_detail = data_obj['drug_detail']
                producer = [i.split('(')[0] for i in drug_detail]
                rels_drug_producer += [[i.split('(')[0], i.split('(')[-1].replace(')', '')] for i in drug_detail]
                producer_jsons += producer
            disease_infos.append(disease_dict)
        return set(drug_jsons), set(food_jsons), set(check_jsons), set(department_jsons), set(producer_jsons), set(
            symptom_jsons), set(disease_jsons), disease_infos, \
               rels_check, rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, rels_drug_producer, rels_recommanddrug, \
               rels_symptom, rels_acompany, rels_category

    '''建立节点'''

    def create_node(self, label, nodes):
        count = 0
        for node_name in nodes:
            node = Node(label, name=node_name)
            self.g.create(node)
            count += 1
            print(count, len(nodes))
        return

    '''创建知识图谱中心疾病的节点'''

    def create_diseases_nodes(self, disease_infos):
        count = 0
        for disease_dict in disease_infos:
            node = Node("Disease", name=disease_dict['name'], desc=disease_dict['desc'],
                        prevent=disease_dict['prevent'], cause=disease_dict['cause'],
                        easy_get=disease_dict['easy_get'], cure_lasttime=disease_dict['cure_lasttime'],
                        cure_department=disease_dict['cure_department']
                        , cure_way=disease_dict['cure_way'], cured_prob=disease_dict['cured_prob'])
            self.g.create(node)
            count += 1
            print(count)
        return

    '''创建知识图谱实体节点类型schema'''

    def create_graphnodes(self):
        Drugs, Foods, Checks, Departments, Producers, Symptoms, Diseases, disease_infos, rels_check, rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, rels_drug_producer, rels_recommanddrug, rels_symptom, rels_acompany, rels_category = self.read_nodes()
        self.create_diseases_nodes(disease_infos)
        self.create_node('Drug', Drugs)
        print(len(Drugs))
        self.create_node('Food', Foods)
        print(len(Foods))
        self.create_node('Check', Checks)
        print(len(Checks))
        self.create_node('Department', Departments)
        print(len(Departments))
        self.create_node('Producer', Producers)
        print(len(Producers))
        self.create_node('Symptom', Symptoms)
        return

    '''创建实体关系边'''

    def create_graphrels(self):
        Drugs, Foods, Checks, Departments, Producers, Symptoms, Diseases, disease_infos, rels_check, rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, rels_drug_producer, rels_recommanddrug, rels_symptom, rels_acompany, rels_category = self.read_nodes()
        self.create_relationship('Disease', 'Food', rels_recommandeat, 'recommand_eat', u'推荐食谱')
        # self.create_relationship('Disease', 'Food', rels_noteat, 'no_eat', u'忌吃')
        # self.create_relationship('Disease', 'Food', rels_doeat, 'do_eat', u'宜吃')
        # self.create_relationship('Department', 'Department', rels_department, 'belongs_to', u'属于')
        # self.create_relationship('Disease', 'Drug', rels_commonddrug, 'common_drug', u'常用药品')
        # self.create_relationship('Producer', 'Drug', rels_drug_producer, 'drugs_of', u'生产药品')
        # self.create_relationship('Disease', 'Drug', rels_recommanddrug, 'recommand_drug', u'好评药品')
        # self.create_relationship('Disease', 'Check', rels_check, 'need_check', u'诊断检查')
        # self.create_relationship('Disease', 'Symptom', rels_symptom, 'has_symptom', u'症状')
        # self.create_relationship('Disease', 'Disease', rels_acompany, 'acompany_with', u'并发症')
        # self.create_relationship('Disease', 'Department', rels_category, 'belongs_to', u'所属科室')

    '''创建实体关联边'''

    def create_relationship(self, start_node, end_node, edges, rel_type, rel_name):
        count = 0
        # 去重处理
        set_edges = []
        for edge in edges:
            set_edges.append('###'.join(edge))
        all = len(set(set_edges))
        for edge in set(set_edges):
            edge = edge.split('###')
            p = edge[0]
            q = edge[1]
            query = "match(p:%s),(q:%s) where p.name='%s'and q.name='%s' create (p)-[rel:%s{name:'%s'}]->(q)" % (
                start_node, end_node, p, q, rel_type, rel_name)
            try:
                self.g.run(query)
                count += 1
                print(rel_type, count, all)
            except Exception as e:
                print(e)
        return

    '''导出数据'''

    def export_data(self):
        Drugs, Foods, Checks, Departments, Producers, Symptoms, Diseases, disease_infos, rels_check, rels_recommandeat, rels_noteat, rels_doeat, rels_department, rels_commonddrug, rels_drug_producer, rels_recommanddrug, rels_symptom, rels_acompany, rels_category = self.read_nodes()
        f_drug = open('dict/drug.txt', 'w+', encoding='utf-8')
        f_food = open('dict/food.txt', 'w+', encoding='utf-8')
        f_check = open('dict/check.txt', 'w+', encoding='utf-8')
        f_department = open('dict/department.txt', 'w+', encoding='utf-8')
        f_producer = open('dict/producer.txt', 'w+', encoding='utf-8')
        f_symptom = open('dict/symptom.txt', 'w+', encoding='utf-8')
        f_disease = open('dict/disease.txt', 'w+', encoding='utf-8')

        f_drug.write('\n'.join(list(Drugs)))
        f_food.write('\n'.join(list(Foods)))
        f_check.write('\n'.join(list(Checks)))
        f_department.write('\n'.join(list(Departments)))
        f_producer.write('\n'.join(list(Producers)))
        f_symptom.write('\n'.join(list(Symptoms)))
        f_disease.write('\n'.join(list(Diseases)))

        data_dicts_to_excel(disease_infos, 'dict/disease_info.xls')

        f_drug.close()
        f_food.close()
        f_check.close()
        f_department.close()
        f_producer.close()
        f_symptom.close()
        f_disease.close()

        return


def data_dicts_to_excel(data_dicts, output_path):
    df = pd.DataFrame(data_dicts)
    df.to_excel(output_path, encoding='utf-8')


    #
    # # 2. csv的写入文件对象
    # csv_file = open(output_path, 'w', encoding='utf-8')
    # # 3.1获取表头所需要的数据
    # sheet_title = data_dicts[0].keys()
    # # 3.2 取所有内容
    # json_values = []
    # for dict in data_dicts:
    #     json_values.append(dict.values())
    #
    # # 4.写入csv文件
    # # 4.1根据文件对象  生成读写器
    # csv_writer = csv.writer(csv_file, dialect='excel')
    #
    # # 4.2 写入表头
    # csv_writer.writerow(sheet_title)
    # # 4.3 写入内容
    # csv_writer.writerows(json_values)
    #
    # # 5.关闭文件
    # csv_file.close()

    print("存完了")


if __name__ == '__main__':
    handler = MedicalGraph()
    print(u"step1:导入图谱节点中")
    # handler.create_graphnodes()
    print(u"step2:导入图谱边中")
    # handler.create_graphrels()
    handler.export_data()
