from sqlalchemy import create_engine

class Connector:
    def connect(self, 
                dialect='oracle',    
                sql_driver='cx_oracle',  
                username=' ... ',        # Oracle username
                password=' ... ',        # Oracle password    
                host='localhost',        
                port=1521,               # default Oracle port
                service='orcl'):         # Oracle SID
        engine_path = f'{dialect}+{sql_driver}://{username}:{password}@{host}:{str(port)}/?service_name={service}'
        engine = create_engine(engine_path)
        return engine
        
