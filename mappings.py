
DB_CONNECTIONS = {
    '_trd': {
        'host': 'compensation-ag.am.ru',
        'database': '_trdHR_buf'
    },
    'Analytics': {
        'host': 'compensation-ag.am.ru',
        'database': 'Analytics'
    },
    'DWH': {
        'host': 'compensation-ag.am.ru',
        'database': 'DWH'
    },
    'Pifagor2UkDev': {
        'host': 'pifagor20-ag.am.ru',
        'database': 'UkDev'
    },
    'PifagorFst': {
        'host': 'pifagor.am.ru',
        'database': 'fst'
    }
}

# === === === ОБРАЗЕЦ
# 'dwh_schema': {
#    'src_system_name': 'Pifagor2',
#    'src_db_name': 'UkDev',
#    'src_connection_name': 'Pifagor2UkDev',
#    'src_schema_name': 'sbl'
#    'dwt_tables' : [
#            'table1',
#            'table2'
#         ]
#    }

DBO_MAPPING = { 
    'stg_p2_ukdev_sbl': {
        'system_name': 'Pifagor2',
        'db_name': 'UkDev',
        'connection_name': 'Pifagor2UkDev',
        'schema_name': 'sbl',
        'dwh_tables': [
        ]
    },
    'stg_p2_ukdev_tab' : {
        'system_name': 'Pifagor2',
        'db_name': 'UkDev',
        'connection_name': 'Pifagor2UkDev',
        'schema_name': 'tab',
        'dwh_tables': [
            'Account',
            'AccountFigure',
            'Asset',
            'AssetCoupon',
            'AssetNominal',
            #'AssetProperty',
            'Benchmark',
            'CalcIndexComposition',
            'Calendar',
            'Cp',
            'Doc',
            'DocSubType',
            'Figure',
            'FigureFund',
            'FigureOrg',
            'FigureQualification',
            'FigureRelation',
            'FinPlan',
            'IndexCompositeShare',
            'IndexList',
            'IndexPassiveManagement',
            'InvestProduct',
            'InvestProductType',
            'ObjectReference',
            'Oper',
            'OperAccount',
            'OperCP',
            'OperPlaceTradeMode',
            'Rate',
            'Role',
            #'TransAsses',
            'TransCur',
        ]
    },
    'stg_p2_ukdev_tcom' : {
        'system_name': 'Pifagor2',
        'db_name': 'UkDev',
        'connection_name': 'Pifagor2UkDev',
        'schema_name': 'tcom',
        'dwh_tables': [
            'ObjectType',
        ]
    },
    'stg_p2_ukdev_tcut1' : {
        'system_name': 'Pifagor2',
        'db_name': 'UkDev',
        'connection_name': 'Pifagor2UkDev',
        'schema_name': 'tcut1',
        'dwh_tables': [
            'CpGroupMembership',
        ]
    },
    # 'stg_p2_ukdev_tcut5': {
    #     'system_name': 'Pifagor2',
    #     'db_name': 'UkDev',
    #     'connection_name': 'Pifagor2UkDev',
    #     'schema_name': 'tcut5',
    #     'dwh_tables': [
    #         'CutResult',
    #     ]
    #},
    'stg_p2_ukdev_tgloss': {
        'system_name': 'Pifagor2',
        'db_name': 'UkDev',
        'connection_name': 'Pifagor2UkDev',
        'schema_name': 'tgloss',
        'dwh_tables': [
            'AccountType',
            'AssetType',
            'Cur',
            'MethodCreditTypeID',
            'MethodDebitTypeID',
            'OperRoleAccount',
            'OperRoleCP',
            'OperType',
            'Role',
            'TransType'
        ]
    },
    'stg_p2_ukdev_timp': {
        'system_name': 'Pifagor2',
        'db_name': 'UkDev',
        'connection_name': 'Pifagor2UkDev',
        'schema_name': 'timp',
        'dwh_tables': [
            'PifagorAccount',
        ]
    },
    'stg_p2_ukdev_tlist': {
        'system_name': 'Pifagor2',
        'db_name': 'UkDev',
        'connection_name': 'Pifagor2UkDev',
        'schema_name': 'tlist',
        'dwh_tables': [
            'AssetMethodNkdID',
            'AssetTypeClassID',
            'AssetTypeIssuerTypeID',
            'BenchmarkTypeID',
            'CommonDatabaseID',
            'CpFundTypeID',
            'CurrencySource',
            'DocTypeID',
            'FigureOrgTypeID',
            'IndexListTypeID',
            'InvestProductStrategyID',
            'PurposeTypeID',
            'RestTypeID',
            'RiskLevelID'
        ]
    },
    # 'stg_p2_ukdev_v': {
    #     'system_name': 'Pifagor2',
    #     'db_name': 'UkDev',
    #     'connection_name': 'Pifagor2UkDev',
    #     'schema_name': 'v',
    #     'dwh_tables': [
    #         'CpInvestProduct'
    #     ]
    # }
    'stg_p1_fst_dbo': {
        'system_name': 'Pifagor1',
        'db_name': 'fst',
        'connection_name': 'PifagorFst',
        'schema_name': 'dbo',
        'dwh_tables': [
            'AnnualCheckStatus',
            'FATCAStatusDescr',
            'Вознаграждение_агента',
            'Документы',
            'Классы_активов',
            'Классы_активов_ИЦБ',
            'Клиенты',
            'Клиенты_адреса',
            'Клиенты_комплаенс',
            'Клиенты_страны',
            'Коды_типов_документов_личности',
            'Места_хранения_по_коду_ИК',
            'Портфель_активов',
            'Регистрация_договоров',
            'Список_операций',
            'Список_счетов_контрагентов',
            'Список_эмитентов',
            'Тип_доходности',
            'Тип_цены_переоценки',
            'Типы_групп_счетов',
            'Типы_контрагентов',
            'Типы_носителей'
        ]
    },
    'stg_p1_fst_dwh': {  # ???
        'system_name': 'Pifagor1',
        'db_name': 'fst',
        'connection_name': 'PifagorFst',
        'schema_name': 'timp',
        'dwh_tables': [
            '',
        ]
    },
}

# ---------------------------------------------------------------------------------------
if __name__ == '__main__':
    pass
