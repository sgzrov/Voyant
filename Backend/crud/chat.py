from Backend.models.chat_data_model import ChatsDB


def create_chat_message(session, conversation_id, user_id, role, content):
    msg = ChatsDB(
        conversation_id = conversation_id,
        user_id = user_id,
        role = role,
        content = content
    )
    session.add(msg)
    session.commit()
    session.refresh(msg)
    return msg


def get_chat_history(session, conversation_id, user_id):
    return session.query(ChatsDB).filter_by(conversation_id = conversation_id, user_id = user_id).order_by(ChatsDB.timestamp).all()


