import logging
import os.path
from sftp import Sftp


def log(l_in_msg):
    l_in_msg = 'FTP: ' + l_in_msg
    print(l_in_msg)
    logging.info(l_in_msg)


def conectar(p_in_ip, p_in_user, p_in_pass):
    try:
        sftp = Sftp(
            hostname=p_in_ip,
            username=p_in_user,
            password=p_in_pass,
        )
        sftp.connect()  # Connect to SFTP
        return sftp
    except Exception as err:
        log(" ERROR conectar: " + str(err))


def desconectar(sftp):
    try:
        sftp.disconnect()
    except Exception as err:
        log(" ERROR DESconectar: " + str(err))


def downloads(sftp, p_in_pathfile, p_in_pathorigem):
    try:
        filename = p_in_pathfile[-15:].replace('/', '')
        try:
            sftp.download(p_in_pathfile, os.path.join(p_in_pathorigem, filename))  # Download files from SFTP
        except Exception as err:
            log(" ERROR: " + str(err))
            return False

        # Faz um copia do arquivo e remover dentro do programa.
        # shutil.copy(filename, p_in_pathorigem)
        # os.remove(filename)
        return True
    except Exception as err:
        log("Erro! : " + str(err))
        return False
