from pprint import pprint
from modules.KeybaseBindingNLP import KeybaseBindingNLP

kb_nlp = KeybaseBindingNLP()
# kb_nlp.run_nlp_on_messages("group_messages_t")
kb_nlp.run_nlp_on_messages("team_messages_t")