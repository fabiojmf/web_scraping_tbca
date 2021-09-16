# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
from collections import OrderedDict
import json

link1 = "http://www.tbca.net.br/base-dados/int_composicao_estatistica.php?cod_produto="
URL = "http://www.tbca.net.br/base-dados/composicao_estatistica.php"

def criar_json_info_estatistica(elementos,item):
    temp = OrderedDict()
    temp["energia"] = '-'
    temp["energia2"] = '-'
    temp["umidade"] = '-'
    temp["carboidrato_total"] = '-'
    temp["carboidrato_disponivel"] = '-'
    temp["proteina"] = '-'
    temp["lipidios"] = '-'
    temp["fibra_alimentar"] = '-'
    temp["alcool"] = '-'
    temp["cinzas"] = '-'
    temp["colesterol"] = '-'
    temp["acidos_graxos_saturados"] = '-'
    temp["acidos_graxos_monoinsaturados"] = '-'
    temp["acidos_graxos_polinsaturados"] = '-'
    temp["acidos_graxos_trans"] = '-'
    temp["calcio"] = '-'
    temp["ferro"] = '-'
    temp["sodio"] = '-'
    temp["magnesio"] = '-'
    temp["fosforo"] = '-'
    temp["potassio"] = '-'
    temp["zinco"] = '-'
    temp["cobre"] = '-'
    temp["selenio"] = '-'
    temp["vitamina_a_re"] = '-'
    temp["vitamina_a_rae"] = '-'
    temp["vitamina_d"] = '-'
    temp["alfa_tocoferol"] = '-'
    temp["tiamina"] = '-'
    temp["riboflavina"] = '-'
    temp["niacina"] = '-'
    temp["vitamina_b6"] = '-'
    temp["vitamina_b12"] = '-'
    temp["vitamina_c"] = '-'
    temp["equivalente_folato"] = '-'
    temp["sal_adição"] = '-'
    temp["acucar_adição"] = '-'
    
    for chave in temp:    
        if len(elementos) == 8:
            colunas = { 
                "unidades":elementos[0],
                "val_por_100g":elementos[1],
                "dp":elementos[2],
                "val_min":elementos[3],
                "val_max":elementos[4],
                "num_dados_util":elementos[5],
                "ref":elementos[6],
                "tipo":elementos[7]
            }
            temp[chave] = colunas
            
    temp.update({"grupo": item[2]})
    temp.update({"nome": item[1]})
    temp.update({"id": item[0]})
    return temp

def ler_sub_paginas_info_estatistica(item):
    s = requests.Session()
    sub_pagina = s.get(link1+item[0])
    sub_info = BeautifulSoup(sub_pagina.content, "html.parser")
    sub_body_rows = sub_info.find_all("table")[0].find_all("tr")[1:]
    for row_num in range(len(sub_body_rows)):
        temp_val = []
        contador = 0
        for row_item in sub_body_rows[row_num].find_all("td"):
            if contador > 0:
                temp_val.append(row_item.text)
            contador+=1
    s.cookies.clear()
    x = criar_json_info_estatistica(temp_val,item)
    return x

def leitor_pagina(endereco):
    page = requests.get(endereco)
    soup = BeautifulSoup(page.content, "html.parser")
    results = soup.find_all("table")
    table = results[0]
    body = table.find_all("tr")
    body_rows = body[1:]
    global all_rows
    for row_num in range(len(body_rows)):
        row = []
        contador = 0
        for row_item in body_rows[row_num].find_all("td"):
            if contador == 0 or contador == 1 or contador == 4:
                row.append(row_item.text)
            contador+=1
        all_rows.append(ler_sub_paginas_info_estatistica(row))
    proximo = soup.find(id="block_2")
    for link in proximo.find_all('a'):
        if 'próxima' in link.contents[0]:
            leitor_pagina("http://www.tbca.net.br/base-dados/"+link['href'])

all_rows = []
leitor_pagina(URL)

with open('composicao_quimica.json', 'w') as outfile:
    json.dump(all_rows, outfile)
