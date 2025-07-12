import openpyxl
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException
import datetime
import sys
import os
import queue
import threading
import time
import traceback
import json

# Configurações globais
EXPIRATION_DATE = datetime.datetime(2025, 7, 15, 20, 0, 0, tzinfo=datetime.timezone.utc)
HEADERS = [
    "code", "name", "pricing", "discount", "pricing_with", "confins_tax", 
    "confins_value", "difalst_tax", "difalst_value", "fecop_tax", "fecop_value", 
    "icmi_value", "icms_tax", "icms_value", "ipi_tax", "ipi_value", "pis_tax", 
    "pis_value", "st_tax", "st_value", "weight"
]
header_labels ={
    "code": "Código",
    "name": "Nome",
    "pricing": "Preço",
    "discount": "Desconto",
    "pricing_with": "Preço com Impostos",
    "cofins_tax": "Cofins",
    "cofins_value": "Cofins Valor",
    "difalst_tax": "Difal ST",
    "difalst_value": "Difal ST Valor",
    "fecop_tax": "Fecop",
    "fecop_value": "Fecop Valor",
    "icmi_value": "ICMI Valor",
    "icms_tax": "ICMS",
    "icms_value": "ICMS Valor",
    "ipi_tax": "IPI",
    "ipi_value": "IPI Valor",
    "pis_tax": "PIS",
    "pis_value": "PIS Valor",
    "st_tax": "ST",
    "st_value": "ST Valor",
    "weight": "Peso"
}
MAX_WORKER_RETRIES = 3  # Máximo de tentativas para reiniciar um worker
WORKER_RESTART_DELAY = 5  # Segundos entre tentativas de reiniciar worker
with open('config.json', 'r') as f:
    config = json.load(f)

NUM_WORKERS = config["num_workers"]


def login():
    """Configura e inicia uma instância do Chrome com logs desativados."""
    options = ChromeOptions()
    options.add_argument('--log-level=3')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1200,800")  # Tamanho fixo para estabilidade

    driver = webdriver.Chrome(options=options)
    driver.get("https://ctshoponline.atlascopco.com/pt-BR/login")
    
    try:
        print("Aceitando cookies...")
        onetrust_accept_btn_handler = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
        onetrust_accept_btn_handler.click()
        
        print("Clicando em 'Conecte-se'...")
        conecte_se_btn = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Conecte-se')]")))
        conecte_se_btn.click()
        
        print("Inserindo email...")
        input_email = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@type='email']")))
        input_email.send_keys("vendas@borbon.com.br")
        
        print("Submetendo email...")
        input_submit = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@type='submit']")))
        input_submit.click()
        
        print("Inserindo senha...")
        input_senha = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@type='password']")))
        input_senha.send_keys("Brb2025!")
        
        print("Clicando em 'Entrar'...")
        button_entrar = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "idSIButton9")))
        button_entrar.click()
        
        print("Recusando permanecer conectado...")
        button_no = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "idBtn_Back")))
        button_no.click()
        
        # Verificação se o login foi bem-sucedido
        print("Verificando login...")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, "//p[contains(., 'Welcome') and .//b[text()='Vendas']]"))
        )
        print("Login bem-sucedido!")
        return driver
    except Exception as e:
        print(f"Erro durante o login: {e}")
        print(traceback.format_exc())
        driver.quit()
        return None

def search_product(driver, term):
    """Busca informações de um produto usando um driver já logado."""
    try:
        print(f"Buscando produto: {term}")
        driver.get(f"https://ctshoponline.atlascopco.com/en-GB/products/{term}")
        
        # Localizadores para os possíveis resultados
        product_name_locator = (By.XPATH, "//h1[@class='mt-2']")
        no_results_locator = (By.XPATH, "//h1[contains(., 'No results for')]")
        not_available_locator = (By.XPATH, "//div[@data-cy='app-notification-alert']")
        resource_not_found_locator = (By.XPATH, "//h2[contains(., 'The server cannot find the requested resource.')]")
        no_longer_available_locator = (By.XPATH, "//*[normalize-space()='The product is no longer available']")

        # Espera por qualquer um dos elementos
        WebDriverWait(driver, 15).until(
            lambda d: d.find_elements(*product_name_locator) or 
                      d.find_elements(*no_results_locator) or 
                      d.find_elements(*not_available_locator) or 
                      d.find_elements(*resource_not_found_locator) or
                      d.find_elements(*no_longer_available_locator)
        )

        # Verifica casos de falha
        if driver.find_elements(*no_results_locator):
            print(f"  -> Nenhum resultado encontrado para: {term}")
            return None
        elif driver.find_elements(*not_available_locator):
            print(f"  -> Produto indisponível: {term}")
            return None
        elif driver.find_elements(*resource_not_found_locator):
            print(f"  -> Recurso não encontrado (404): {term}")
            return None
        elif driver.find_elements(*no_longer_available_locator):
            print(f"  -> Produto não está mais disponível: {term}")
            return None

        # Se chegou aqui, o produto foi encontrado
        product = {"name": driver.find_element(*product_name_locator).text}
        print(f"  -> Produto encontrado: {product['name']}")

        # Extração de preços
        print("  Extraindo informações de preço...")
        button_pricing = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Pricing')]")))
        driver.execute_script("arguments[0].click();", button_pricing)

        WebDriverWait(driver, 60).until(
            EC.text_to_be_present_in_element((By.XPATH, "(//div[@role='tabpanel']//td)[1]"), "BRL")
        )
        
        tds = [td.text for td in driver.find_elements(By.XPATH, "//div[@role='tabpanel']//td")]
        product["pricing"] = tds[0].split(" ")[1]
        product["discount"] = "0" if tds[1] == "-" else tds[1]
        product["pricing_with"] = tds[2].split(" ")[1]
        
        # Extração de impostos
        print("  Extraindo informações de impostos...")
        button_taxes = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Taxes')]")))
        driver.execute_script("arguments[0].click();", button_taxes)
        
        table = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "table")))
        
        cells = [cell.text for cell in table.find_elements(By.XPATH, ".//td[@data-cy='informationTableCell']")]
        
        # Processamento robusto de dados fiscais
        tax_data = {
            "confins": cells[1] if len(cells) > 1 else "",
            "difalst": cells[3] if len(cells) > 3 else "",
            "fecop": cells[5] if len(cells) > 5 else "",
            "icmi": cells[7] if len(cells) > 7 else "",
            "icms": cells[9] if len(cells) > 9 else "",
            "ipi": cells[11] if len(cells) > 11 else "",
            "pis": cells[13] if len(cells) > 13 else "",
            "st": cells[15] if len(cells) > 15 else "",
        }
        
        # Função auxiliar para processar valores fiscais
        def parse_tax_value(value_str):
            if "% (BRL " in value_str:
                parts = value_str.split("% (BRL ")
                return parts[0], parts[1].replace(")", "")
            elif "BRL " in value_str:
                return "", value_str.split("BRL ")[1]
            else:
                return "", value_str

        # Atribuição dos valores
        product["confins_tax"], product["confins_value"] = parse_tax_value(tax_data["confins"])
        product["difalst_tax"], product["difalst_value"] = parse_tax_value(tax_data["difalst"])
        product["fecop_tax"], product["fecop_value"] = parse_tax_value(tax_data["fecop"])
        _, product["icmi_value"] = parse_tax_value(tax_data["icmi"])
        product["icms_tax"], product["icms_value"] = parse_tax_value(tax_data["icms"])
        product["ipi_tax"], product["ipi_value"] = parse_tax_value(tax_data["ipi"])
        product["pis_tax"], product["pis_value"] = parse_tax_value(tax_data["pis"])
        product["st_tax"], product["st_value"] = parse_tax_value(tax_data["st"])
        
        # Informações do produto
        print("  Extraindo informações do produto...")
        button_info = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Product information')]")))
        driver.execute_script("arguments[0].click();", button_info)
        
        weight_cell = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "(//td[@data-cy='informationTableCell'])[2]")))
        product["weight"] = weight_cell.text
        
        print(f"  -> Dados coletados para {term}")
        return product
        
    except (TimeoutException, StaleElementReferenceException) as e:
        print(f"  -> Erro temporário durante busca de {term}: {type(e).__name__}")
        raise  # Relança para tratamento específico
        
    except Exception as e:
        print(f"  -> ERRO durante processamento de {term}:")
        print(traceback.format_exc())
        return None

def worker(worker_id, driver, code_queue, results_queue, errors_queue):
    """Processa códigos da fila usando um driver persistente."""
    print(f"Worker {worker_id}: Iniciado")
    
    while True:
        try:
            # Tenta obter um código da fila com timeout
            term = code_queue.get(timeout=30)
            print(f"Worker {worker_id}: Processando código {term}")
            
            try:
                # Tenta buscar o produto
                product_data = search_product(driver, term)
                
                if product_data:
                    product_data["code"] = term
                    results_queue.put(product_data)
                    print(f"Worker {worker_id}: Sucesso com código {term}")
                else:
                    print(f"Worker {worker_id}: Produto {term} não encontrado ou indisponível")
                
                # Marca o código como processado
                code_queue.task_done()
                
            except (TimeoutException, StaleElementReferenceException):
                # Erros temporários - recoloca o código na fila
                print(f"Worker {worker_id}: Erro temporário com {term} - Recolocando na fila")
                errors_queue.put(term)
                code_queue.task_done()
                
                # Reinicia o driver para limpar estado
                print(f"Worker {worker_id}: Reiniciando driver...")
                try:
                    driver.quit()
                except:
                    pass
                
                driver = login()
                if not driver:
                    print(f"Worker {worker_id}: Falha crítica no relogin. Saindo...")
                    break
                
            except Exception as e:
                print(f"Worker {worker_id}: Erro inesperado com {term}:")
                print(traceback.format_exc())
                code_queue.task_done()
    
        except queue.Empty:
            # Fila vazia por mais de 30 segundos - finaliza worker
            print(f"Worker {worker_id}: Sem códigos por 30 segundos. Finalizando...")
            break
            
    # Limpeza final
    try:
        driver.quit()
    except:
        pass
    print(f"Worker {worker_id}: Finalizado")

def get_completed_codes(filepath="results.xlsx"):
    """Lê os códigos já processados diretamente do arquivo de resultados do Excel."""
    if not os.path.exists(filepath):
        return set()
    try:
        workbook = openpyxl.load_workbook(filepath)
        sheet = workbook.active
        # Lê a primeira coluna (códigos), pulando o cabeçalho
        return {str(row[0].value) for row in sheet.iter_rows(min_row=2, max_col=1) if row[0].value}
    except Exception as e:
        print(f"Aviso: Não foi possível ler os códigos do arquivo de resultados: {e}")
        return set()

def setup_results_file(filepath, headers):
    """Cria arquivo de resultados se não existir."""
    if not os.path.exists(filepath):
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.append(headers)
        workbook.save(filepath)

def save_product_data(product_data, filepath):
    """Salva dados de um produto no Excel."""
    workbook = openpyxl.load_workbook(filepath)
    sheet = workbook.active
    row = [product_data.get(header, "") for header in HEADERS]
    sheet.append(row)
    workbook.save(filepath)

def main():
    """Função principal com gerenciamento de filas e workers."""
    # Verificação de data de expiração
    if datetime.datetime.now(datetime.timezone.utc) > EXPIRATION_DATE:
        print("ERRO: O script expirou em 15/07/2025 e não pode mais ser executado.")
        sys.exit(1)

    # Carrega códigos a processar
    workbook_input = openpyxl.load_workbook('lista.xlsx')
    sheet_input = workbook_input.worksheets[0]
    all_codes = [str(row[0]).zfill(10) for row in sheet_input.iter_rows(
        min_row=2, max_col=1, values_only=True) if row[0]]
    
    processed_codes = get_completed_codes()
    codes_to_process = [code for code in all_codes if code not in processed_codes]
    
    print(f"{len(processed_codes)} códigos já processados. {len(codes_to_process)} a processar.")
    
    if not codes_to_process:
        print("Nenhum código novo para processar.")
        return

    # Prepara arquivo de resultados
    results_filepath = "results.xlsx"
    setup_results_file(results_filepath, HEADERS)

    # Cria filas
    code_queue = queue.Queue()
    results_queue = queue.Queue()
    errors_queue = queue.Queue()
    
    # Preenche fila de códigos
    for code in codes_to_process:
        code_queue.put(code)

    # Cria e inicia workers com tratamento robusto
    workers = []
    active_workers = []
    worker_threads = {}
    
    # Função para iniciar um worker
    def start_worker(worker_id):
        print(f"\nIniciando worker {worker_id}...")
        driver = login()
        if driver:
            t = threading.Thread(
                target=worker,
                args=(worker_id, driver, code_queue, results_queue, errors_queue),
                daemon=True
            )
            t.start()
            worker_threads[worker_id] = t
            active_workers.append(worker_id)
            print(f"Worker {worker_id} iniciado com sucesso")
            return True
        return False
    
    # Tenta iniciar todos os workers com múltiplas tentativas
    for i in range(NUM_WORKERS):
        worker_id = i + 1
        attempts = 0
        success = False
        
        while not success and attempts < MAX_WORKER_RETRIES:
            success = start_worker(worker_id)
            if not success:
                attempts += 1
                print(f"Falha ao iniciar worker {worker_id}, tentativa {attempts}/{MAX_WORKER_RETRIES}")
                time.sleep(WORKER_RESTART_DELAY)
        
        if not success:
            print(f"CRÍTICO: Não foi possível iniciar worker {worker_id} após {MAX_WORKER_RETRIES} tentativas")

    # Processa resultados e erros
    try:
        while True:
            # Verifica e reinicia workers que falharam
            for worker_id in list(active_workers):
                if not worker_threads[worker_id].is_alive():
                    print(f"Worker {worker_id} morreu. Tentando reiniciar...")
                    active_workers.remove(worker_id)
                    attempts = 0
                    success = False
                    
                    while not success and attempts < MAX_WORKER_RETRIES:
                        success = start_worker(worker_id)
                        if not success:
                            attempts += 1
                            print(f"Falha ao reiniciar worker {worker_id}, tentativa {attempts}/{MAX_WORKER_RETRIES}")
                            time.sleep(WORKER_RESTART_DELAY)
                    
                    if not success:
                        print(f"CRÍTICO: Não foi possível reiniciar worker {worker_id}")
            
            # Processa resultados bem-sucedidos
            while not results_queue.empty():
                product_data = results_queue.get()
                save_product_data(product_data, results_filepath)
                results_queue.task_done()
            
            # Processa erros (recoloca códigos na fila)
            while not errors_queue.empty():
                code = errors_queue.get()
                print(f"Recolocando código {code} na fila")
                code_queue.put(code)
                errors_queue.task_done()
            
            # Verifica se o processamento terminou
            if code_queue.empty() and not any(t.is_alive() for t in worker_threads.values()):
                break
                
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nInterrupção recebida. Finalizando...")
    
    # Recoloca erros pendentes na fila principal
    while not errors_queue.empty():
        code = errors_queue.get()
        code_queue.put(code)
        errors_queue.task_done()

    # Aguarda conclusão da fila de códigos
    code_queue.join()
    print("Todos os códigos processados.")
    
    # Garante processamento final
    while not results_queue.empty():
        product_data = results_queue.get()
        save_product_data(product_data, results_filepath)
        results_queue.task_done()

    print("Processo concluído com sucesso.")

if __name__ == "__main__":
    main()
    
    
    """
            # Verificação se o login foi bem-sucedido
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//p[contains(., 'Welcome') and .//b[text()='Vendas']]"))
        )
        return driver
    """