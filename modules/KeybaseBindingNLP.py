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
                CREATE TABLE IF NOT EXISTS nlp_messages(message_id, result_json)
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
            # Check Message already in nlp_messages
            # message ID is msg.id + msg.conversation_id
            for message in message_set:
                message = json.loads(message[0])
                if message["msg"]["content"]["type"] == "text":
                    message_id = str(message["msg"]["id"]) + message["msg"]["conversation_id"]
                    num_messages = self.cur.execute(f"""
                        SELECT COUNT(*) FROM nlp_messages
                        WHERE message_id = '{message_id}';
                        """).fetchone()
                    if num_messages[0] == 0:
                        # run NLP
                        nlp_results = self.nlp(message["msg"]["content"]["text"]["body"])
                        input_sql_results = []
                        if len(nlp_results) !=  0:
                            for result in nlp_results:
                                input_sql_results.append((message_id, json.dumps(str(result))))
                                print(f"Processing {(message_id, json.dumps(str(result)))}")
                            # Insert NLP results
                            self.cur.executemany("INSERT INTO nlp_messages(message_id, result_json) VALUES(?, json(?))", input_sql_results)
                            self.con.commit()
                        else:
                             self.cur.execute(f"INSERT INTO nlp_messages(message_id) VALUES('{message_id}')").fetchone()