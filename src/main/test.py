from main import Main

if __name__ == "__main__":
    # Configurações iniciais
    list_of_tables = [9942, 9936]
    create_remote_directory = False
    conecting_db = False

    # Inicializa a classe principal e executa o processamento
    main_process = Main(list_of_tables, create_remote_directory, conecting_db)
    main_process.main()