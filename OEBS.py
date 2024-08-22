# libs
import logging
import arquivo_ftp
import cx_Oracle
from datetime import datetime

c_dias = 2

# Dados Oracle Banco de dados
g_ebs_tns_dese = ""
g_ebs_tns_prod = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=produgscan.ultra.corp)(PORT=1521)) (CONNECT_DATA= (SERVICE_NAME=PROD) (INSTANCE_NAME=PROD3) ) )"
g_ebs_user = "consulta"  # bloqueado
g_ebs_pass = "UCons#2017$"  # bloqueado
g_ebs_tns = g_ebs_tns_prod

# Dados FTP
g_ftp_ip_048 = 'ugbrmtzslx048'
g_ftp_ip_131 = 'ugbrslbslx131'
g_ftp_ip_132 = 'ugbrslbslx132'
g_ftp_user = g_ftp_pass = 'transf'

g_concurrentes_filtro = "('CSTUBSAPLBRT', 'CSTU_AR_RET_BANCARIO_REG','CSTU_AR_PEDBAI_TITPAG','CSTU_AR_GERAARQ_TIT_VAVEN_PA', 'CSTUARLOADZOOMNFE', 'CSTUCGMPREVABASTGRISCO', 'CSTU_APROVA_CTRC', 'CSTU_ARGAP191', 'CSTU_AR_ATUALIZA_STATUS', 'CSTU_AR_CREATE_BOL_LAYOUT', 'CSTU_AR_GERA_OCORREN_REG_P', 'CSTU_AR_GERA_REMESSA_REG_P', 'CSTU_BSARREJ_REL', 'CSTU_CAN_OS_AJUSTE_AJC', 'CSTU_CE_RECONCILIACAO_AUTO', 'CSTU_CSI_MOVIM_BASE', 'CSTU_IEX_ARQ_RETORNO', 'CSTU_IEX_PROC_FILE', 'CSTU_MI_EXTRACT_REPORT', 'CSTU_MI_EXT_REPORT_EMAIL', 'CSTU_MI_IFACE_REP_MKT_CLOUD', 'CSTU_OM_CANC_SHIP_CONF', 'CSTU_OM_CV_IMP_BENEFIC', 'CSTU_PO_DEPARA_ITEM_FORNEC', 'CSTU_QP_PRICE_LIST_SEND_EMAIL', 'CSTU_REPLICA_EXTRATO_POSICAO_P', 'CSTU_UPD_METPAG_PEDBAI', 'P_OMINT001', 'P_OMINT002_1') "


def log(l_in_msg):
    l_in_msg = 'OEBS: ' + l_in_msg
    print(l_in_msg)
    logging.info(l_in_msg)


def conectar_db():
    # return cx_Oracle.connect(user=g_ebs_user, password=g_ebs_pass, dsn=g_ebs_tns)
    return cx_Oracle.connect(dsn=g_ebs_tns)


def processar(p_in_path, p_in_resquestid):
    global l_servidor, l_logfile, l_outfile
    try:
        log(f"Processando Id:[{p_in_resquestid}] : " + p_in_path + ' : ' + str(datetime.now().strftime('%d/%m/%Y-%H:%M:%S')))
        conn = conectar_db()
        c = conn.cursor()
        c.execute("SELECT count(1) from apps.fnd_concurrent_requests r  "
                  "WHERE 1=1" \
                  "and r.request_date > sysdate - " + str(c_dias) + \
                  "and r.request_id = " + str(p_in_resquestid))
        listregisters = c.fetchall()
        qtd_registers = list(listregisters[:])[0][0]
        log(f"Exite {str(qtd_registers)} registro com MENOS de 2 dias para o Request: " + p_in_resquestid + "!")

        if qtd_registers > 0:
            l_sql = "SELECT to_char(r.request_date, 'dd/mm/yyyy hh24:mi:ss') data, r.status_code status, r.logfile_name, r.outfile_name, SUBSTR(r.logfile_node_name,  -3) servidor, r.argument_text  from apps.fnd_concurrent_requests r " \
                    "WHERE 1=1" \
                    "and r.request_date > sysdate - " + str(c_dias) + \
                    "and r.request_id = " + str(p_in_resquestid)
            c.execute(l_sql)
            return_dados_sql = c.fetchall()
            conn.close()

            for row in return_dados_sql:
                # l_data = str(row[0])
                # l_status = str(row[1])
                l_logfile = str(row[2])
                l_outfile = str(row[3])
                l_servidor = str(row[4])
                # l_argument = str(row[5])

            if l_servidor == '131':
                g_ftp_ip = g_ftp_ip_131
            elif l_servidor == '048':
                g_ftp_ip = g_ftp_ip_048
            elif l_servidor == '132':
                g_ftp_ip = g_ftp_ip_132
            else:
                log("Servidor: [" + l_servidor + "] nao processar.")
                return False

            lista_arquivo = list()
            lista_arquivo.append(l_logfile)
            lista_arquivo.append(l_outfile)

            # conectar APENAS 1x
            cox_unica_sftp = arquivo_ftp.conectar(g_ftp_ip, g_ftp_user, g_ftp_pass)

            # Conectar e copia o arquivo
            for arquivo in lista_arquivo:  # for dos 2 arquivos ok
                if arquivo_ftp.downloads(cox_unica_sftp, arquivo, p_in_path):
                    log(f"Arquivo [{arquivo}] copiado com sucesso!")

            # desconectar
            arquivo_ftp.desconectar(cox_unica_sftp)

            return True  # Tem registro/copiado/movido OK
        return False  # Nao tem registros
    except Exception as err:
        log("Conexao com erro! : " + str(err))
    finally:
        log('finalizando')


def buscar_quantidade_iderro_p():
    global conn
    try:
        conn = conectar_db()
        c = conn.cursor()
        c.execute(
            "SELECT COUNT(1) FROM apps.fnd_concurrent_programs_vl a, apps.fnd_executables_vl b, apps.fnd_concurrent_requests c, apps.fnd_user d, apps.fnd_responsibility_tl e "
            "WHERE c.status_code IN ('D', 'E', 'T', 'U', 'X', 'G') AND c.phase_code NOT IN ('R') "
            "AND a.concurrent_program_name IN " + g_concurrentes_filtro +
            "AND b.executable_id = a.executable_id AND c.concurrent_program_id(+) = a.concurrent_program_id AND d.user_id(+) = c.requested_by AND e.responsibility_id(+) = c.responsibility_id AND e.language(+) = 'PTB' "
            "AND c.actual_start_date >= SYSDATE - " + str(c_dias))
        list_registers = c.fetchall()
        qtd_registers = list(list_registers[:])[0][0]
        log(f"Exitem {str(qtd_registers)} registros com erro (lista de concurrents) " + str(datetime.now().strftime('%d/%m/%Y-%H:%M:%S')))
        return int(qtd_registers)
    except Exception as err:
        log("erro buscar_quantidade_iderro_p! : " + str(err))
    finally:
        conn.close()


def buscar_lista_iderro_p():
    global conn
    try:
        conn = conectar_db()
        c = conn.cursor()
        c.execute(
            "SELECT c.request_id, to_CHAR(c.actual_start_date, 'ddmmyyyy_hh24miss') data_execucao, a.concurrent_program_name, a.user_concurrent_program_name FROM apps.fnd_concurrent_programs_vl a, apps.fnd_executables_vl b, apps.fnd_concurrent_requests c, apps.fnd_user d, apps.fnd_responsibility_tl e "
            "WHERE c.status_code IN ('D', 'E', 'T', 'U', 'X', 'G') AND c.phase_code NOT IN ('R') "
            "AND a.concurrent_program_name IN " + g_concurrentes_filtro +
            "AND b.executable_id = a.executable_id AND c.concurrent_program_id(+) = a.concurrent_program_id AND d.user_id(+) = c.requested_by AND e.responsibility_id(+) = c.responsibility_id AND e.language(+) = 'PTB' "
            "AND c.actual_start_date >= SYSDATE - " + str(c_dias))
        list_registers = c.fetchall()
        return list_registers
    except Exception as err:
        log("Erro buscar_lista_iderro_p : " + str(err))
    finally:
        conn.close()
