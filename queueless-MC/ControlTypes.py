from DefectType import DefectType

class ACC03(DefectType):
    def __init__(self, sub_log_df_ACC03):
        super().__init__('ACC03', sub_log_df_ACC03)
        # self.control_type = 'ACC03'
        # self.sub_log_df = sub_log_df_ACC03

class ACC17(DefectType):
    def __init__(self, sub_log_df_ACC17):
        super().__init__('ACC17', sub_log_df_ACC17)

class ACC28(DefectType):
    def __init__(self, sub_log_df_ACC28):
        super().__init__('ACC28', sub_log_df_ACC28)

class AUTH18(DefectType):
    def __init__(self, sub_log_df_AUTH18):
        super().__init__('AUTH18', sub_log_df_AUTH18)

class AUTH42(DefectType):
    def __init__(self, sub_log_df_AUTH42):
        super().__init__('AUTH42', sub_log_df_AUTH42)