import requests
import mysql.connector
import json
import os
import datetime
import os
from datetime import datetime

def verificar_execucao():
    """Verifica se o script já rodou hoje"""
    try:
        mydb = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        cursor = mydb.cursor()

        data_hoje = datetime.now().date()
        cursor.execute("SELECT COUNT(*) FROM execution_log WHERE last_run = %s", (data_hoje,))
        resultado = cursor.fetchone()[0]

        cursor.close()
        mydb.close()

        if resultado > 0:
            print("Script já rodou hoje. Saindo...")
            return True  # Já executado hoje
        return False  # Ainda não executado hoje

    except mysql.connector.Error as err:
        print(f"Erro ao verificar execução: {err}")
        return False


def registrar_execucao():
    """Registra a execução do script"""
    try:
        mydb = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        cursor = mydb.cursor()

        data_hoje = datetime.now().date()
        cursor.execute("INSERT INTO execution_log (last_run) VALUES (%s)", (data_hoje,))
        mydb.commit()

        print("Execução registrada com sucesso.")

        cursor.close()
        mydb.close()
    except mysql.connector.Error as err:
        print(f"Erro ao registrar execução: {err}")

if verificar_execucao():
    exit()  # Se já rodou hoje, sai do script
    
print("Executando script...")  # Apenas para teste

main_url = "https://console.springserve.com/api/v0/"
auth_url = f"{main_url}auth"

email = os.getenv("SPRINGSERVE_EMAIL")
password = os.getenv("SPRINGSERVE_PASSWORD")

payload = json.dumps({"email": email, "password": password})
headers = {"Content-Type": "application/json"}

try:
    auth_response = requests.post(auth_url, headers=headers, data=payload)
    auth_response.raise_for_status()  # Verifica se a autenticação foi bem-sucedida

    auth_data = auth_response.json()
    access_token = auth_data.get("token")

    if access_token:
        print("Token:", access_token)

        report_url = f"{main_url}report"
        report_headers = {"Authorization": f"{access_token}"}
        print("Cabeçalho de autorização:", report_headers)

        try:
            mydb = mysql.connector.connect(
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_NAME"),
            )
            cursor = mydb.cursor()
            nome_tabela = "springserve"

            # filtros = [{"type": "SupplyTag", "id": "846641"}]
            params = {
            "date_range": "yesterday",
            "interval": "day",
            "dimensions": [
                "device_id",
                "channel_name",
                "content_custom1_param",
                "content_episode",
                "content_genre",
                "content_id",
                "content_series",
                "content_title",
                "language",
                "device_brand",
                "supply_tag_id"
            ],
            "metrics": [
                "total_requests",
                "usable_requests",
                "total_impressions",
                "fill_rate",
                "revenue",
                "cost",
                "rpm",
                "cpm",
                "net_ppm"
            ],
            "supply_tag_ids": ["846641", "881829", "881835", "881834", "881833"],
            "async": "false"
        }

            try:
                # Usando POST para a requisição do relatório
                report_response = requests.post(
                    report_url,
                    headers=report_headers,
                    json=params,  # Usando json=params para enviar os parâmetros no corpo da requisição
                )
                report_response.raise_for_status()

                report_data = report_response.json()

                try:
                    for dado in report_data:
                        colunas = "(date, content_genre, content_title, content_episode, content_id, channel_name, language, content_custom_1_param, requests, imps, req_fill_percent, revenue, rpm, cpm, ppm, device_brand, cost, supply_tag_id)"
                        valores = (
                            dado.get("date"),
                            dado.get("content_genre"),
                            dado.get("content_title"),
                            dado.get("content_episode"),
                            dado.get("content_id"),
                            dado.get("channel_name"),
                            dado.get("language"),
                            dado.get("content_custom1_param"),
                            dado.get("usable_requests"),
                            dado.get("total_impressions"),
                            dado.get("fill_rate"),
                            dado.get("revenue"),
                            dado.get("rpm"),
                            dado.get("cpm"),
                            dado.get("net_ppm"),
                            dado.get("device_brand"),
                            dado.get("cost"),
                            dado.get("supply_tag_id")
                        )
                        sql = f"INSERT INTO {nome_tabela} {colunas} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        cursor.execute(sql, valores)

                    mydb.commit()
                    print(f"Dados inseridos na tabela {nome_tabela} com sucesso!")

                except mysql.connector.Error as e:
                    print(f"Erro durante a inserção no MySQL: {e}")
                    mydb.rollback()
                except Exception as e:
                     print(f"Erro na inserção: {e}")

            except requests.exceptions.RequestException as e:
                print(f"Erro na requisição do relatório: {e}")
            except json.JSONDecodeError as e:
                print(f"Erro ao decodificar JSON: {e}")
            except Exception as e:
                print(f"Erro na requisição: {e}")

        except mysql.connector.Error as e:
            print(f"Erro na conexão com o MySQL: {e}")
        except Exception as e:
            print(f"Erro durante o processo: {e}")

    else:
        print("Falha na autenticação. Token não encontrado.")

except requests.exceptions.RequestException as e:
    print(f"Erro na requisição de autenticação: {e}")
except json.JSONDecodeError as e:
    print(f"Erro ao decodificar JSON: {e}")
except Exception as e:
    print(f"Erro durante o processo: {e}")

finally:
    if "mydb" in locals() and mydb.is_connected():
        cursor.close()
        mydb.close()
        print("Conexão com o MySQL fechada.")
        

registrar_execucao()  # Atualiza o arquivo com a data da execução