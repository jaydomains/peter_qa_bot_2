from peter.interfaces.telegram.state import ConversationState
from peter.interfaces.telegram.llm_dialog import call_llm

state = ConversationState(chat_id=99999)

response1 = call_llm(state, "I want to create a new site")
print("Test 1 response:", response1)

response2 = call_llm(
    state,
    "Site code is JHB001, name is Sandton Tower, address is 1 Sandton Drive",
)
print("Test 2 response:", response2)
