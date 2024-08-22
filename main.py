# This is a sample Python script.
from datetime import datetime
from time import sleep
import os
import threading
import logging
import OEBS

g_dir_local = 'C:\\TMP\\logRequestId\\'
logfile = logging.getLogger("Main")
l_nome_log = g_dir_local + 'logRequestId_' + datetime.now().strftime('%d%m%Y_%H%M%S') + '.log'
logging.basicConfig(filename=l_nome_log, filemode='w', encoding='utf-8', level=logging.INFO)


def log(l_in_msg):
    l_in_msg = 'Main: ' + l_in_msg
    print(l_in_msg)
    logging.info(l_in_msg)


def iniciazar_variaveis(p_in_requestId):
    global l_dia_atual
    try:
        l_dia_atual = datetime.now().strftime('%d%m%Y')

        # criar a pasta diaria
        if not os.path.exists(g_dir_local + l_dia_atual):
            os.makedirs(g_dir_local + l_dia_atual)

        # criar o idexeucao
        if not os.path.exists(g_dir_local + l_dia_atual + '\\' + p_in_requestId):
            os.makedirs(g_dir_local + l_dia_atual + '\\' + p_in_requestId)
            return g_dir_local + l_dia_atual + '\\' + p_in_requestId

    except Exception as err:
        log("Erro iniciazar_variaveis : " + str(err))
    finally:
        return g_dir_local + l_dia_atual + '\\' + p_in_requestId


def diario():
    # busca a quantidade de registro para processar e a lista somente com id com erro
    qtdRegisters = OEBS.buscar_quantidade_iderro_p()
    lista_erro = list(OEBS.buscar_lista_iderro_p())
    i = 0

    for lista_requestid_erro in lista_erro:
        log("")
        requestidErro = lista_requestid_erro[0]
        dataexecucao = lista_requestid_erro[1]
        nomeconcurrent = lista_requestid_erro[2]
        nomeconcurrentusuario = lista_requestid_erro[3]

        i = i + 1
        log("Processando " + str(i) + "/" + str(qtdRegisters) + "  [" + str(
            requestidErro) + "] [" + nomeconcurrent + "] [" + nomeconcurrentusuario + "]")

        # return Boolean, porem nao fazemos nada, apenas para pular para o proximo.
        if OEBS.processar(iniciazar_variaveis(str(requestidErro) + "_" + nomeconcurrent + "_" + dataexecucao), str(requestidErro)):
            x = threading.Thread()
            x.start()
            sleep(3)
        else:
            log("Nao processado, favor verificar o detalhe do log.")


def requests(p_in_request):
    lista_requestId = str(p_in_request).split(',')  # converte a entrada em umas lista para ser processada 1 por 1

    for resquest_id in lista_requestId:
        OEBS.processar(iniciazar_variaveis(resquest_id), resquest_id)  # return Boolean, porem nao fazemos nada, apenas para pular para o proximo.
        x = threading.Thread()
        x.start()
        sleep(3)


def principal():
    entrada = input("RequestID:")

    if entrada != '':
        requests(entrada)
    else:
        diario()


# bloco principal
# schedule.every().hour.do(principal())
# while True:
#    schedule.run_pending()
#    datetime.sleep(10)
principal()
log("Fim.")
