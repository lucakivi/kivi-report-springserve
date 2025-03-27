import requests
import mysql.connector
import json
import os
import datetime

db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

def was_already_run_today():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SELECT last_run FROM execution_log ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()
    conn.close()
    return result and result[0] == datetime.date.today()

def update_run_log():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO execution_log (last_run) VALUES (%s)", (datetime.date.today(),))
    conn.commit()
    conn.close()

if not was_already_run_today():
    print("Executando script...")
    # Seu código principal aqui (baixar os reports, salvar no DB, etc.)
    update_run_log()
else:
    print("Script já rodou hoje. Saindo...")



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
                "device_brand"
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
            "supply_tag_ids": ["846641"],
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
                        colunas = "(date, content_genre, content_title, content_episode, content_id, channel_name, language, content_custom_1_param, requests, imps, req_fill_percent, revenue, rpm, cpm, ppm, device_brand, cost)"
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
                            dado.get("cost")
                        )
                        sql = f"INSERT INTO {nome_tabela} {colunas} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
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