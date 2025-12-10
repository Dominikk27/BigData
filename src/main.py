from Database.DatabaseManager import connect_database, check_table


def main():
    db_conn = connect_database()
    check_table(db_conn, "plants_metrics")



if __name__ == "__main__":
    main()
