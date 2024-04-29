from database.DB_connect import DBConnect
from model.situazione import Situazione

class MeteoDao():

    def get_all_sitauzioni(mese):
        cnx = DBConnect.get_connection()
        result = []
        if cnx is None:
            print("Connessione fallita")
        else:
            cursor = cnx.cursor(dictionary=True)
            query = """SELECT s.Localita, s.Data, s.Umidita
                        FROM Situazione s 
                        GROUP BY s.Data ASC"""
            cursor.execute(query, (mese,))
            for row in cursor:
                result.append((row["Localita"], row["media"]))
            cursor.close()
            cnx.close()
        return result

    @staticmethod
    def get_umidita_media_mese(mese):
        cnx = DBConnect.get_connection()
        result = []
        if cnx is None:
            print("Connessione fallita")
        else:
            cursor = cnx.cursor(dictionary=True)
            query = """SELECT s.Localita, AVG(s.Umidita)
                        FROM Situazione s 
                        WHERE MONTH(s.'Data')=&s
                        GROUP BY s.Localita"""
            cursor.execute(query, (mese,))
            for row in cursor:
                result.append((row["Localita"], row["media"]))
            cursor.close()
            cnx.close()
        return result


    def get_situazione_meta_mese(mese):
        cnx = DBConnect.get_connection()
        result = []
        if cnx is None:
            print("Connessione fallita")
        else:
            cursor = cnx.cursor(dictionary=True)
            query = """SELECT s.Localita, s.Data, s.Umidita
                        FROM Situazione s 
                        WHERE MONTH(s.'Data')=&s
                        AND DAY(s.Data)<=15
                        GROUP BY s.Data ASC"""
            cursor.execute(query, (mese,))
            for row in cursor:
                result.append((row["Localita"], row["media"]))
            cursor.close()
            cnx.close()
        return result


