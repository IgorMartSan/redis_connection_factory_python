import redis
import atexit
import pickle

class RedisConnection:
    def __init__(self, host, port, bd):

        self.__host = host  # os.getenv('REDIS_HOST')
        self.__port = port  # int(os.getenv('REDIS_PORT'))
        self.__bd = bd  # int(os.getenv('REDIS_DB'))

        self.redis_client = redis.Redis(
            host=self.__host,
            port=self.__port,
            db=self.__bd
        )

        #atexit.register(self.__destroy_connection)

    def get_connection(self):

        return self.redis_client

    def __destroy_connection(self):

        print("Connection terminated")
        self.redis_client.close()


    ###################################################################################
        

    def add_to_stream(self, data:dict, stream_key:str,  max_len:int = 50):
 

            serialized_data = pickle.dumps(data)
            entry_id = self.redis_client.xadd(
                stream_key,
                {"data_serialized": serialized_data},
                maxlen=max_len,
                approximate=True
            )

            return entry_id

    def get_from_stream(self, stream_key: str, last_id:str="0") -> dict:

        # Lê uma única mensagem do stream no Redis
        stream_messages = self.redis_client.xread(
            {stream_key: last_id}, count=1, block=0)

        if stream_messages:
            stream_name, message_list = stream_messages[0]
            message = message_list[0]

            # Obtém o ID da mensagem atual
            currentID = message[0]

            # Extrai os dados serializados da mensagem
            data_serialized = message[1][b'data_serialized']

            # Desserializa os dados usando a biblioteca pickle
            deserialized_data = pickle.loads(data_serialized)

            # Atualiza o último ID lido
            last_id = currentID

            return deserialized_data, last_id

        # Retorna None se não houver mensagens
        return None

    def get_stream_length(self, stream_name):
o do stream.
        """
        stream_length = self.redis_client.xlen(stream_name)
        return stream_length
    


    #######################################################################

    def insert_dict_to_redis(self, key, data):

        serialized_data = pickle.dumps(data)
        self.redis_client.set(key, serialized_data)

    def get_dict_from_redis(self, key):

        serialized_data = self.redis_client.get(key)
        if serialized_data:
            deserialized_data = pickle.loads(serialized_data)
            return deserialized_data
        else:
            return None
