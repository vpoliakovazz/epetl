import pandas as pd
import gspread

from data_extraction import Extract, DataOperation
import os, ftplib, socket, ssl, zipfile
from io import StringIO


FTPTLS_OBJ = ftplib.FTP_TLS

# Class to manage implicit FTP over TLS connections, with passive transfer mode
# - Important note:
#   If you connect to a VSFTPD server, check that the vsftpd.conf file contains
#   the property require_ssl_reuse=NO
class FTPTLS(FTPTLS_OBJ):

    host = "127.0.0.1"
    port = 990
    user = "anonymous"
    timeout = 60

    logLevel = 0

    # Init both this and super
    def __init__(self, host=None, user=None, passwd=None, acct=None, keyfile=None, certfile=None, context=None, timeout=60):
        FTPTLS_OBJ.__init__(self, host, user, passwd, acct, keyfile, certfile, context, timeout)

    # Custom function: Open a new FTPS session (both connection & login)
    def openSession(self, host="127.0.0.1", port=990, user="anonymous", password=None, timeout=60):
        self.user = user
        # connect()
        ret = self.connect(host, port, timeout)
        # prot_p(): Set up secure data connection.
        try:
            ret = self.prot_p()
            if (self.logLevel > 1): self._log("INFO - FTPS prot_p() done: " + ret)
        except Exception as e:
            if (self.logLevel > 0): self._log("ERROR - FTPS prot_p() failed - " + str(e))
            raise e
        # login()
        try:
            ret = self.login(user=user, passwd=password)
            if (self.logLevel > 1): self._log("INFO - FTPS login() done: " + ret)
        except Exception as e:
            if (self.logLevel > 0): self._log("ERROR - FTPS login() failed - " + str(e))
            raise e
        if (self.logLevel > 1): self._log("INFO - FTPS session successfully opened")

    # Override function
    def connect(self, host="127.0.0.1", port=990, timeout=60):
        self.host = host
        self.port = port
        self.timeout = timeout
        try:
            self.sock = socket.create_connection((self.host, self.port), self.timeout)
            self.af = self.sock.family
            self.sock = ssl.wrap_socket(self.sock, self.keyfile, self.certfile)
            self.file = self.sock.makefile('r')
            self.welcome = self.getresp()
            if (self.logLevel > 1): self._log("INFO - FTPS connect() done: " + self.welcome)
        except Exception as e:
            if (self.logLevel > 0): self._log("ERROR - FTPS connect() failed - " + str(e))
            raise e
        return self.welcome

    # Override function
    def makepasv(self):
        host, port = FTPTLS_OBJ.makepasv(self)
        # Change the host back to the original IP that was used for the connection
        host = socket.gethostbyname(self.host)
        return host, port

    # Custom function: Close the session
    def closeSession(self):
        try:
            self.close()
            if (self.logLevel > 1): self._log("INFO - FTPS close() done")
        except Exception as e:
            if (self.logLevel > 0): self._log("ERROR - FTPS close() failed - " + str(e))
            raise e
        if (self.logLevel > 1): self._log("INFO - FTPS session successfully closed")

    # Private method for logs
    def _log(self, msg):
        # Be free here on how to implement your own way to redirect logs (e.g: to a console, to a file, etc.)
        print(msg)


class TransformationAPI:
    def __init__(self):
        self.operation_obj = DataOperation()
        self.extract_sql = Extract()

    def test(self, table_name):
        # getattr function takes in function name of class and calls it.
        return getattr(self, table_name)()

    @staticmethod
    def gsh_managers():

        gc = gspread.service_account()
        sh = gc.open("Імпорт")
        df = pd.DataFrame(sh.sheet1.get('B:F'))
        df.columns = df.iloc[0]
        df = df[1:]
        df.columns = ["company_id", "company_title", "km", "mm", "sm"]
        print(df.shape)
        return df

    @staticmethod
    def for_epicentrm_orders():
        host = "213.160.154.49"
        port = 8021
        user = "epicentrm"
        password = "qTW1KqlEHg"

        myFtps = FTPTLS()
        myFtps.logLevel = 2
        myFtps.openSession(host, port, user, password)
        print(myFtps.retrlines("LIST"))

        dest_dir = "C:/projects/etl/epic-etl/etl_epic/ftps_dir/"
        with open(os.path.join(dest_dir, 'for_epicentrm_orders.zip'), "wb") as f:
            myFtps.retrbinary("RETR {}".format('for_epicentrm_orders.zip'), f.write)

        myFtps.closeSession()

        zf = zipfile.ZipFile('C:/projects/etl/epic-etl/etl_epic/ftps_dir/for_epicentrm_orders.zip')
        df = zf.read('for_epicentrm_orders.json')
        s = str(df, 'utf-8')

        data = StringIO(s)
        data = pd.read_json(data)
        data = pd.DataFrame(data["for_epicentrm"]["orders"])
        data["feo_id"] = data["order_number"] + data["art_1c"]
        data = data.drop_duplicates(subset="feo_id", keep="last")
        data = data[data['feo_id'].notna()]
        data = data[['feo_id', 'order_id_1c', 'order_number', 'client_order', 'order_date',
                     'doc_status', 'is_cancel', 'delivery_method', 'good_id_1c', 'art_1c',
                     'quantity', 'sell_amt']]

        return data

    @staticmethod
    def for_epicentrm_goods():
        host = "213.160.154.49"
        port = 8021
        user = "epicentrm"
        password = "qTW1KqlEHg"

        myFtps = FTPTLS()
        myFtps.logLevel = 2
        myFtps.openSession(host, port, user, password)
        print(myFtps.retrlines("LIST"))

        dest_dir = "C:/projects/etl/epic-etl/etl_epic/ftps_dir/"
        with open(os.path.join(dest_dir, 'for_epicentrm_goods.zip'), "wb") as f:
            myFtps.retrbinary("RETR {}".format('for_epicentrm_goods.zip'), f.write)

        myFtps.closeSession()

        zf = zipfile.ZipFile('C:/projects/etl/epic-etl/etl_epic/ftps_dir/for_epicentrm_goods.zip')
        df = zf.read('for_epicentrm_goods.json')
        s = str(df, 'utf-8')

        data = StringIO(s)
        data = pd.read_json(data)
        data = pd.DataFrame(data["for_epicentrm"]["goods"])
        data = data[['good_id_1c', 'art_1c', 'good_name', 'marketplace', 'deleted']]
        return data
