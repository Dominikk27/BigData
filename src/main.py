from Database.DatabaseManager import connect_database, checkTable


def main():
    db_conn = connect_database()
    checkTable(db_conn, "plants_metrics")



if __name__ == "__main__":
    main()