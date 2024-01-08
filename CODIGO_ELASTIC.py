import timeit
from elasticsearch import Elasticsearch, helpers
from statistics import mean
import matplotlib.pyplot as plt
import time

cliente = Elasticsearch(
    'https://localhost:9200',
    basic_auth=('usuario123', 'senha123'),
    ca_certs='C:\\Users\\maxue\\Music\\elasticsearch-8.11.3\\config\\certs\\http_ca.crt',
)


nome_indice = 'indice'

def splitar_linha_arquivo(linha_arquivo):
    linha_splitada = []
    linha_arquivo = linha_arquivo.split('.T\n')
    linha_splitada.append(linha_arquivo[0].replace('\n', '').strip())
    linha_arquivo = linha_arquivo[1].split('.A\n')
    linha_splitada.append(linha_arquivo[0].replace('\n', ''))
    linha_arquivo = linha_arquivo[1].split('.B\n')
    linha_splitada.append(linha_arquivo[0].replace('\n', ''))
    linha_arquivo = linha_arquivo[1].split('.W\n')
    linha_splitada.append(linha_arquivo[1].replace('\n', ' '))
    return linha_splitada


def indexar_arquivos(nome_arquivo, nome_indice, cliente):
    cliente.indices.create(index=nome_indice)
    documentos_indexar = []
    with open(nome_arquivo) as arquivo:
        arquivos_splitado = arquivo.read().split('.I')[1:]
    for linha in arquivos_splitado:
        linha_arquivo = splitar_linha_arquivo(linha)
        index = linha_arquivo[0]
        titulo = linha_arquivo[1]
        autor = linha_arquivo[2]
        texto_arquivo = linha_arquivo[3]
        documento = {
            '_op_type': 'index',
            '_index': nome_indice,
            '_id': int(index),
            '_source': {
                'titulo': titulo,
                'autor': autor,
                'texto_arquivo': texto_arquivo,
            }
        }
        documentos_indexar.append(documento)
    helpers.bulk(client=cliente, actions=documentos_indexar)

    #esperando os arquivos serem indexados depois de enviados
    while True:
        resultado = cliente.cat.indices(index=nome_indice, format='json')[0]
        if int(resultado['docs.count']) == 1400:
            return
        time.sleep(0.5)


def obter_documentos_relevantes(nome_arquivo):
    relevantes = {}
    with open(nome_arquivo, 'r') as arquivo:
        linhas = arquivo.read().split('\n')
        for linha in linhas:
            linha = linha.split(' ')
            if linha[0] not in relevantes:
                relevantes[linha[0].strip()] = [linha[1].strip()]
            else:
                relevantes[linha[0].strip()].append(linha[1].strip())
    return relevantes


def buscar_arquivos(nome_arquivo, nome_indice, cliente: Elasticsearch):
    resultados = {}
    with open(nome_arquivo) as arquivo:
        arquivos_splitados = arquivo.read().split('.I')[1:]
        for x, arquivo in enumerate(arquivos_splitados):
            busca = cliente.search(index=nome_indice,
                                   query={
                                       'multi_match': {
                                           'query': arquivo.split('.W\n')[1],
                                           'fields':
                                           ['titulo', 'autor', 'corpo']
                                       }
                                   })
            resultado_busca = busca['hits']['hits']
            indice = '{}'.format(x + 1)
            resultados[indice] = []
            for resultado in resultado_busca:
                resultados[indice].append(resultado['_id'].strip())
    return resultados


tempo_antes_indexacao = timeit.default_timer()
indexar_arquivos('cran.all.1400', nome_indice, cliente)
tempo_depois_indexacao = timeit.default_timer()

print('Tempo de indexar os arquivos: ',
      tempo_depois_indexacao - tempo_antes_indexacao)

tempo_antes_busca = timeit.default_timer()
resultados = buscar_arquivos('cran.qry', nome_indice, cliente)
tempo_depois_busca = timeit.default_timer()

print('Tempo de buscar os arquivos: ', tempo_depois_busca - tempo_antes_busca)

#calculando precisão e recall
documentos_relevantes = obter_documentos_relevantes('cranqrel')
precisoes = []
recalls = []
tamanho = len(documentos_relevantes)

for k in range(1, 11):
    precisoes_k = []
    recalls_k = []
    for i in range(tamanho):
        indice = '{}'.format(i + 1)
        documentos_k_relevantes = len(
            set(documentos_relevantes[indice]).intersection(
                resultados[indice][:k]))
        precisoes_k.append(documentos_k_relevantes / k)
        recalls_k.append(documentos_k_relevantes /
                         len(documentos_relevantes[indice]))
    precisoes.append(mean(precisoes_k))
    recalls.append(mean(recalls_k))

#plotando grafico precisao
plt.plot(range(1, 11), precisoes, marker='o')
plt.title('Elastic Search Precisao @ k')
plt.xlabel('k')
plt.ylabel('precisao')
plt.show()

#plotando grafico recall
plt.plot(range(1, 11), recalls, marker='o')
plt.title('Elastic Search Recall @ k')
plt.xlabel('k')
plt.ylabel('Recall')
plt.show()

cliente.indices.delete(index=nome_indice)
