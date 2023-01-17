import sqlite3
import json
from pprint import pprint
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline


class KeybaseBindingNLP():
    def __init__(self, sqlite_path="./keybase_export.sqlite"):
        self.con = sqlite3.connect(sqlite_path)
        self.cur = self.con.cursor()
        self.validate_data_and_create_table()
        self.load_nlp()

    def validate_data_and_create_table(self):
        results = self.cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        tables = []
        for item in results:
            tables.append(item[0])
        if "team_messages_t" in tables or "group_messages_t" in tables:
            results = self.cur.execute("""
                CREATE TABLE IF NOT EXISTS nlp_team_messages_t(
                    msg_pkey,
                    team_name, 
                    topic_name, 
                    msg_id, 
                    result_json)
                """).fetchall()
            results = self.cur.execute("""
                CREATE TABLE IF NOT EXISTS nlp_group_messages_t(
                    msg_pkey,
                    group_name, 
                    msg_id, 
                    result_json)
                """).fetchall()
            return True
        else:
            raise ValueError('Missing tables team_messages_t and group_messages_t that hold messages.')

    def load_nlp(self):
        self.tokenizer = AutoTokenizer.from_pretrained("dslim/bert-base-NER")
        self.model     = AutoModelForTokenClassification.from_pretrained("dslim/bert-base-NER")
        self.nlp       = pipeline("ner", model=self.model, tokenizer=self.tokenizer)

    def run_nlp_on_messages(self, message_table):
        num_messages = self.cur.execute(f"""
            SELECT COUNT(*) FROM {message_table}
            WHERE json_extract(message_json, '$.msg.content.type') = 'text';
            """).fetchone()[0]
        print(num_messages)
        for offset in range(0, num_messages-1, 100):
            message_set = self.cur.execute(f"""
                SELECT message_json FROM {message_table}
                WHERE json_extract(message_json, '$.msg.content.type') = 'text'
                LIMIT 100 OFFSET {str(offset)};
                """).fetchmany(100)
            # Check Message already in nlp_messages_t
            # message ID is msg.id + msg.conversation_id
            for message in message_set:
                message = json.loads(message[0])
                if message["msg"]["content"]["type"] == "text":
                    message_id = str(message["msg"]["id"]) + message["msg"]["conversation_id"]
                    num_messages = self.cur.execute(f"""
                        SELECT COUNT(*) FROM nlp_messages_t
                        WHERE message_id = '{message_id}';
                        """).fetchone()
                    if num_messages[0] == 0:
                        # run NLP
                        nlp_results = self.nlp(message["msg"]["content"]["text"]["body"])
                        input_sql_results = []
                        if len(nlp_results) !=  0:
                            for result in nlp_results:
                                result["score_str"] = str(result["score"])
                                result["score"] = int(result["score"] * 1000000)
                                input_sql_results.append((message_id, json.dumps(result)))
                                print(f"Processing {(message_id, json.dumps(result))}")
                                pprint(json.dumps(result))
                            # Insert NLP results
                            sql_query = "INSERT INTO nlp_messages_t(message_id, result_json) VALUES(?, json(?))"
                            self.cur.executemany(sql_query, input_sql_results)
                            self.con.commit()
                        else:
                            sql_query = f"INSERT INTO nlp_messages_t(message_id) VALUES('{message_id}')"
                            self.cur.execute().fetchone()
                            
    
    def run_nlp_on_team_messages(self):
        num_messages = self.cur.execute(f"""
            SELECT COUNT(*) FROM team_messages_t
            WHERE json_extract(message_json, '$.msg.content.type') = 'text';
            """).fetchone()[0]
        print(num_messages)
        for offset in range(0, num_messages-1, 100):
            message_set = self.cur.execute(f"""
                SELECT message_json FROM team_messages_t
                WHERE json_extract(message_json, '$.msg.content.type') = 'text'
                LIMIT 100 OFFSET {str(offset)};
                """).fetchmany(100)
            # Check Message already in nlp_messages_t
            # message ID is msg.id + msg.conversation_id
            for message in message_set:
                message = json.loads(message[0])
                if message["msg"]["content"]["type"] == "text":
                    msg_pkey = str(message["msg"]["id"]).zfill(6) + "-" +message["msg"]["conversation_id"]
                    num_messages = self.cur.execute(f"""
                        SELECT COUNT(*) FROM nlp_team_messages_t
                        WHERE msg_pkey = '{msg_pkey}';
                        """).fetchone()
                    if num_messages[0] == 0:
                        # Run NLP
                        nlp_results = self.nlp(message["msg"]["content"]["text"]["body"])
                        team_name   = message["msg"]["channel"]["name"]
                        topic_name  = message["msg"]["channel"]["topic_name"]
                        msg_id      = message["msg"]["id"]
                        input_sql_results = []
                        if len(nlp_results) !=  0:
                            for result in nlp_results:
                                result["score_str"] = str(result["score"])
                                result["score"] = int(result["score"] * 1000000)
                                input_sql_results.append((msg_pkey, team_name, topic_name, msg_id, json.dumps(result)))
                                print(f"Processing {(msg_pkey, json.dumps(result))}")
                                pprint(json.dumps(result))
                            # Insert NLP results
                            sql_query = "INSERT INTO nlp_team_messages_t(msg_pkey, team_name, topic_name, msg_id, result_json) VALUES(?, ?, ?, ?, json(?))"
                            self.cur.executemany(sql_query, input_sql_results)
                            self.con.commit()
                        else:
                            sql_query = f"INSERT INTO nlp_team_messages_t(msg_pkey) VALUES('{msg_pkey}')"
                            self.cur.execute(sql_query).fetchone()

    
    def run_nlp_on_group_messages(self):
        num_messages = self.cur.execute(f"""
            SELECT COUNT(*) FROM group_messages_t
            WHERE json_extract(message_json, '$.msg.content.type') = 'text';
            """).fetchone()[0]
        print(num_messages)
        for offset in range(0, num_messages-1, 100):
            message_set = self.cur.execute(f"""
                SELECT group_name, message_json FROM group_messages_t
                WHERE json_extract(message_json, '$.msg.content.type') = 'text'
                LIMIT 100 OFFSET {str(offset)};
                """).fetchmany(100)
            # Check Message already in nlp_messages_t
            # message ID is msg.id + msg.conversation_id
            for message in message_set:
                group_name = message[0]
                message = json.loads(message[1])
                if message["msg"]["content"]["type"] == "text":
                    msg_pkey = str(message["msg"]["id"]).zfill(6) + "-" +message["msg"]["conversation_id"]
                    num_messages = self.cur.execute(f"""
                        SELECT COUNT(*) FROM nlp_group_messages_t
                        WHERE msg_pkey = '{msg_pkey}';
                        """).fetchone()
                    if num_messages[0] == 0:
                        # Run NLP
                        nlp_results = self.nlp(message["msg"]["content"]["text"]["body"])
                        msg_id      = message["msg"]["id"]
                        input_sql_results = []
                        if len(nlp_results) !=  0:
                            for result in nlp_results:
                                result["score_str"] = str(result["score"])
                                result["score"] = int(result["score"] * 1000000)
                                input_sql_results.append((msg_pkey, group_name, msg_id, json.dumps(result)))
                                print(f"Processing {(msg_pkey, json.dumps(result))}")
                                pprint(json.dumps(result))
                            # Insert NLP results
                            sql_query = "INSERT INTO nlp_group_messages_t(msg_pkey, group_name, msg_id, result_json) VALUES(?, ?, ?, json(?))"
                            self.cur.executemany(sql_query, input_sql_results)
                            self.con.commit()
                        else:
                            sql_query = f"INSERT INTO nlp_group_messages_t(msg_pkey) VALUES('{msg_pkey}')"
                            self.cur.execute(sql_query).fetchone()
