from aiogram.fsm.state import State, StatesGroup

# Стани для FSM
class RegistrationStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    waiting_for_phone_to_delete = State()  # Додано стан для видалення аккаунта

class GroupStates(StatesGroup):
    waiting_for_group_name = State()
    waiting_for_group_id = State()
    waiting_for_group_list = State()
    waiting_for_package_name = State()
    waiting_for_single_group_id = State()
    waiting_for_account_selection = State()

class BroadcastStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_group_selection = State()
    waiting_for_account_selection = State()
    waiting_for_confirmation = State()

class JoinGroupsStates(StatesGroup):
    waiting_for_group_ids = State()
    waiting_for_interval = State()
    waiting_for_account_selection = State()
    waiting_for_random_interval_config = State()

class MassBroadcastStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_interval = State()
    waiting_for_random_settings = State()
    waiting_for_package_selection = State()
    waiting_for_message_interval_config = State()
    waiting_for_different_messages = State()
    waiting_for_account_message = State()
    waiting_for_broadcast_mode = State()
    waiting_for_message_type = State()
    waiting_for_media_file = State()
    waiting_for_media_caption = State()
    waiting_for_single_group_id = State()

class DeletePackageStates(StatesGroup):
    waiting_for_package_name = State()
