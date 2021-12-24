#!/home/sgd/jsonmerger/p3/bin/python

import json
import ijson
import codecs
import decimal
import glob
import logging
import traceback
import os, sys
from itertools import groupby

logger = logging.getLogger('Webscrapper')
logger.setLevel(logging.INFO)

consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)

logger.addHandler(consoleHandler)

formatter = logging.Formatter(u'%(asctime)s %(levelname)s: %(message)s')
consoleHandler.setFormatter(formatter)

ORGAO="IBAMA"

campos = {}
with codecs.open('/dados/'+ORGAO+'/dados/bpm/CAMPO.json', 'rb', encoding='utf8') as json_file:

    campo_json = json.load(json_file)

    for k in campo_json:
        if ('DES_LABEL' or 'DES_NOME' in k) and (k['DES_LABEL'].strip() != ''):
            campos[k['DES_NOME']] = k['DES_LABEL']

logger.info('Campo carregado')

# Busca form dos processos
processos = {}
with codecs.open('/dados/'+ORGAO+'/dados/bpm/PROCESSO.json', encoding="utf-8") as processos_file:
    form = ijson.items(processos_file, 'item')
    forms = (o for o in form)

    for k,v in groupby(forms, lambda x:x['COD_PROCESSO']):
        list_inter = list(v)
        for item in list_inter:
            processos[str(item['COD_PROCESSO'])] = {}
            processos[str(item['COD_PROCESSO'])]['FORM_VERSAO'] = str(item['COD_FORM'])+'_'+str(item['COD_VERSAO'])
            processos[str(item['COD_PROCESSO'])]['IDE_FINALIZADO'] = item['IDE_FINALIZADO']

logger.info('Processo carregado')

# Busca nome das etapas
etapas = {}
with codecs.open('/dados/'+ORGAO+'/dados/bpm/ETAPA.json', encoding="utf-8") as etapas_file:
    form = ijson.items(etapas_file, 'item')
    etapas_list = (o for o in form)

    for etapa in etapas_list:
        etapas[str(etapa['COD_ETAPA'])+'_'+str(etapa['COD_FORM'])+'_'+str(etapa['COD_VERSAO'])] = etapa['TITULO_ETAPA']

logger.info('Etapas Carregadas')

#Busca nome dos usuarios
usuarios = {}
with codecs.open('/dados/'+ORGAO+'/dados/bpm/USUARIO.json') as forms_file:
    form = ijson.items(forms_file, 'item')
    forms = (o for o in form)

    for item in forms:
        try:
            usuarios[item['COD_USUARIO']] = item
        except Exception as e:
            logger.exception('Usuários não carregados corretamente')
            pass

logger.info('Usuarios carregados')


# Busca data dos processos das etapas
processos_etapas = {}
with codecs.open('/dados/'+ORGAO+'/dados/bpm/PROCESSO_ETAPA.json', encoding="utf-8") as processos_etapas_file:
    form = ijson.items(processos_etapas_file, 'item')
    processos_etapas_list = (o for o in form)

    for processo_etapa in processos_etapas_list:
        processo_etapa['DES_LOGIN'] = usuarios[processo_etapa['COD_USUARIO_ETAPA']]['DES_LOGIN']
        processo_etapa['DES_EMAIL'] = usuarios[processo_etapa['COD_USUARIO_ETAPA']]['DES_EMAIL']
        processo_etapa['NOM_USUARIO'] = usuarios[processo_etapa['COD_USUARIO_ETAPA']]['NOM_USUARIO']
        processos_etapas[str(processo_etapa['COD_PROCESSO'])+'_'+str(processo_etapa['COD_CICLO'])+'_'+str(processo_etapa['COD_ETAPA'])] = processo_etapa


logger.info('Processos Etapas Carregadas')

#Busca nome dos servicos
formularios = {}
with codecs.open('/dados/'+ORGAO+'/dados/bpm/FORMULARIO.json') as forms_file:
    form = ijson.items(forms_file, 'item')
    forms = (o for o in form)

    for item in forms:
        try:
            formularios[str(item['COD_FORM'])+'_'+str(item['COD_VERSAO'])] = item['DES_TITULO']
        except Exception as e:
            logger.exception('Formulários não carregados com sucesso')
            pass

logger.info('Formularios carregados')

servicos = glob.glob('/dados/'+ORGAO+'/dados/bpm/f_*')
for servico in servicos:
    nome_servico = servico.split(sep='f_', maxsplit=1)[1][:-5]
    logger.info('Serviço a ser processado: ' +nome_servico)
    with codecs.open(servico, encoding="utf-8") as json_file:
        result = {}
        try:
            form = ijson.items(json_file, 'item')
            forms = (o for o in form)
            for list_v in forms:
                k = list_v['COD_PROCESSO_F']
                try:
                    result[k]['id'] = k
                    proces = processos[str(k)]['FORM_VERSAO']
                except Exception as e:
                    result[k] = {}
                    result[k]['id'] = k
                    proces = processos[str(k)]['FORM_VERSAO']
                    result[k]['form_versao'] = proces
                    result[k]['IDE_FINALIZADO'] = processos[str(k)]['IDE_FINALIZADO']
                    result[k]['DES_TITULO'] = formularios[str(proces)]
                list_inter = dict(list_v)
                for item in dict(list_v):
                    try:
                        if campos[item] not in list_inter:
                            list_inter[item+"|"+campos[item]] = list_inter[item]
                        else:
                            list_inter[item+"|"+item] = list_inter[item]
                        if item != campos[item]:
                            del list_inter[item]
                    except Exception as e:
                        pass

                # Adiciona nome da etapa para cada metadado
                list_inter['TITULO_ETAPA'] = etapas[str(list_inter['COD_ETAPA_F'])+'_'+proces]
                # Adiciona metadados dos processos das etapas
                list_inter['PROCESSOS_ETAPAS'] = processos_etapas[str(list_inter['COD_PROCESSO_F'])+'_'+str(list_inter['COD_CICLO_F'])+'_'+str(list_inter['COD_ETAPA_F'])]

                try:
                    result[k]['metadados'].append(list_inter)
                except Exception as e:
                    result[k]['metadados'] = [list_inter]
                    pass

            logger.info('Resultado completado')

            grids = glob.glob('/dados/'+ORGAO+'/dados/bpm/g_'+nome_servico+'G*')
            for grid in grids:
                nome_grid = grid.split(sep='g_'+nome_servico, maxsplit=1)[1][:-5]
                logger.info('Grid a ser processada: ' +nome_grid)
                # Pode acontecer de um nome do servico ser substring de outro servico. Isso pode acarretar problemas ao carregar GRIDS.
                # Uma forma correta é ter essas GRIDs obtidas em alguma tabela
                # Como pratica o nome das grids sempre comecam pela letra G
                if nome_grid[0].isupper():
                    with codecs.open(grid, encoding="utf-8") as json_file:
                        try:
                            grid_item = json.load(json_file)
                            for item in grid_item:
                                proces = result[item['COD_PROCESSO']]['form_versao']
                                item['TITULO_ETAPA'] = etapas[str(item['COD_ETAPA'])+'_'+proces]
                                try:
                                    result[item['COD_PROCESSO']][nome_grid].append(item)
                                except Exception as e:
                                    result[item['COD_PROCESSO']][nome_grid] = [item]
                        except Exception as e:
                            logger.info('Arquivo ' + grid + ' está vazio')
                            pass

            logger.info('Grid completo')

            class ComplexEncoder(json.JSONEncoder):
                def default(self, obj):
                    if isinstance(obj, decimal.Decimal):
                        return str(obj)
                    # Let the base class default method raise the TypeError
                    return json.JSONEncoder.default(self, obj)


            resultado = {}
            resultado['resposta'] = []
            for k in result.keys():
                resultado['resposta'].append(result[k])
            result = {}

            os.makedirs('/dados/'+ORGAO+'/json', exist_ok=True)
            with open('/dados/'+ORGAO+'/json/'+nome_servico+'.json', 'w') as outfile:
                json.dump(resultado, outfile, cls=ComplexEncoder)
            resultado = {}
        except Exception as e:
            logger.exception('Arquivo ' + servico + ' está vazio')
            pass
