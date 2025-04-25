import configparser
import my_encrypt
import getpass
import os

version = 20240722

keys_to_not_prompt = [
    "password",
    "jdbc_driver", 
    "jdbc_driver_file"
]

section_enable_dict = {}


def test_section(section):
    if section.startswith('IDMC_CDI'):
        section = 'IDMC_CDI'
    result = True
    try:
        if section_enable_dict[section].upper().startswith('F'):
            result = False
    except Exception:
        pass

    return result



def prompt_update_config(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    
    updated = False
   
    for section in config.sections():
        for key in config[section]:
            if ( key not in keys_to_not_prompt ) :
                new_value = ""
                if key.startswith('encrypted') and test_section(section):
                    print(f"Current value of [{section}] {key}: {config[section][key]}")
                    new_value = getpass.getpass(f"    Enter new password for [{section}] {key} (leave blank to keep current value): ")
                elif key.startswith('enable'):
                    print(f"Current value of [{section}] {key}: {config[section][key]}")
                    section_enable_dict[section] = config[section][key]
                    raw_value = input(f"    Enter new value for [{section}] {key} (leave blank to keep current value): ")
                    if raw_value.upper().startswith('Y') or raw_value.upper().startswith('T') or raw_value.upper().startswith('1'):
                        new_value = 'True'
                        section_enable_dict[section] = True
                    elif raw_value.upper().startswith('N') or raw_value.upper().startswith('F') or raw_value.upper().startswith('0'):
                        new_value = 'False'
                        section_enable_dict[section] = False
                elif test_section(section):
                    print(f"Current value of [{section}] {key}: {config[section][key]}")
                    new_value = input(f"    Enter new value for [{section}] {key} (leave blank to keep current value): ")
                    if config[section][key] == 'True' or config[section][key] == 'False':
                        if new_value.upper().startswith('Y') or new_value.upper().startswith('T') or new_value.upper().startswith('1'):
                            new_value = 'True'
                        elif new_value.upper().startswith('N') or new_value.upper().startswith('F') or new_value.upper().startswith('0'):
                            new_value = 'False'
                if new_value:
                    if key.startswith('encrypted'):
                         config[section][key] = my_encrypt.encrypt_message(new_value)
                    else:
                         config[section][key] = new_value
                    updated = True
    
    if updated:
        with open(file_path, 'w') as configfile:
            config.write(configfile)
        print(f"Configuration updated and saved to {file_path}")
    else:
        print("No updates made to the configuration.")


def display_config_with_decrypted_values(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    for each_section in config.sections():
        for (each_key, each_val) in config.items(each_section):
            if each_key.startswith('encrypted'):
                raw_pass = 'XXX_BAD_PASSWORD_XXX'
                try: 
                    raw_pass = my_encrypt.decrypt_message(each_val)
                except Exception:
                    pass
                print(f"[{each_section}] {each_key} = {raw_pass} (DECRYPTED)")
            else:
                print(f"[{each_section}] {each_key} = {each_val}")



# Example usage

if not os.path.exists('secret.key'):
    print("Generating new secret key")
    my_encrypt.generate_key()
file_path = 'config.ini'
update_values = input("Update config.ini? (Y/N): ")
if update_values.upper() == 'Y':
    print("##################################################################################")
    prompt_update_config(file_path)
    print("##################################################################################")

view_values = input("View config.ini, including unencrypted raw passwords? (Y/N): ")
if view_values.upper() == 'Y':
    print()
    print("##################################################################################")
    display_config_with_decrypted_values(file_path)
    print("##################################################################################")
