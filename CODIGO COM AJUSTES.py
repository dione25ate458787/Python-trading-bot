import pandas as pd
import os
import time
from binance.client import Client
from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET
from dotenv import load_dotenv
import logging
from binance.exceptions import BinanceAPIException, BinanceOrderException, BinanceOrderMinAmountException

load_dotenv()

api_key = os.getenv("KEY_BINANCE")
secret_key = os.getenv("SECRET_BINANCE")

cliente_binance = Client(api_key, secret_key)

codigo_operado = "SOLBRL"
ativo_operado = "SOL"
periodo_candle = Client.KLINE_INTERVAL_15MINUTE

# === Parâmetros Configuráveis ===
porcentagem_capital_risco = 0.02  # % do capital em BRL para arriscar por trade
stop_loss_percentual = 0.05      # % de Stop Loss
take_profit_percentual = 0.10   # % de Take Profit
periodo_media_rapida = 9
periodo_media_devagar = 21
# ================================

quantidade_compra_fixa = 0.019 # Quantidade fixa anterior (agora será dinâmico, mas mantive para referência ou testes)

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Obtém informações do ativo uma única vez
try:
    symbol_info = cliente_binance.get_symbol_info(codigo_operado)
    lot_size_filter = next(f for f in symbol_info["filters"] if f["filterType"] == "LOT_SIZE")
    step_size = float(lot_size_filter["stepSize"])
    logging.info(f"Step Size para {codigo_operado}: {step_size}")
except BinanceAPIException as e:
    logging.error(f"Erro ao obter informações do símbolo {codigo_operado}: {e}")
    exit() # Encerra o programa se não conseguir obter informações iniciais

# Obtém saldo inicial uma única vez
try:
    conta = cliente_binance.get_account()
    saldos = {ativo["asset"]: float(ativo["free"]) for ativo in conta["balances"]}
    saldo_inicial_ativo = saldos.get(ativo_operado, 0)
    saldo_inicial_brl = saldos.get("BRL", 0)
    logging.info(f"Saldo inicial de {ativo_operado}: {saldo_inicial_ativo}")
    logging.info(f"Saldo inicial de BRL: {saldo_inicial_brl}")
except BinanceAPIException as e:
    logging.error(f"Erro ao obter informações da conta: {e}")
    exit() # Encerra se não conseguir obter o saldo inicial

def pegando_dados(codigo, intervalo):
    """Obtém dados de preços do ativo."""
    try:
        candles = cliente_binance.get_klines(symbol=codigo, interval=intervalo, limit=100)
        precos = pd.DataFrame(candles, columns=["tempo_abertura", "abertura", "maxima", "minima", "fechamento",
                                                "volume", "tempo_fechamento", "moedas_negociadas", "numero_trades",
                                                "volume_ativo_base_compra", "volume_ativo_cotação", "-"])
        precos["fechamento"] = precos["fechamento"].astype(float)
        precos["tempo_fechamento"] = pd.to_datetime(precos["tempo_fechamento"], unit="ms").dt.tz_localize("UTC")
        precos["tempo_fechamento"] = precos["tempo_fechamento"].dt.tz_convert("America/Sao_Paulo")

        return precos
    except BinanceAPIException as e:
        logging.error(f"Erro ao obter dados de {codigo} - {intervalo}: {e}")
        return None # Retorna None em caso de erro para ser tratado na função principal


def logica_compra(cliente_binance, codigo_ativo, ativo_operado, step_size, saldos, porcentagem_capital_risco):
    """Executa a lógica de compra."""
    try:
        saldo_brl = saldos.get("BRL", 0)
        if saldo_brl <= 0:
            logging.warning("Saldo em BRL insuficiente para compra.")
            return None, None  # Retorna None para order_id e preco_compra

        # Obtém preço atual do ativo para cálculo dinâmico da quantidade
        preco_ativo_info = cliente_binance.get_symbol_ticker(symbol=codigo_ativo)
        preco_ativo = float(preco_ativo_info["price"])

        valor_em_brl_risco = saldo_brl * porcentagem_capital_risco
        quantidade_compra_dinamica = valor_em_brl_risco / preco_ativo

        # Ajusta a quantidade para o step_size
        quantidade_compra_dinamica = float(format(quantidade_compra_dinamica - (quantidade_compra_dinamica % step_size), '.8f'))

        if quantidade_compra_dinamica <= 0:
            logging.warning("Quantidade de compra calculada é zero ou negativa após ajuste de step size.")
            return None, None

        order = cliente_binance.create_order(
            symbol=codigo_ativo, side=SIDE_BUY, type=ORDER_TYPE_MARKET, quantity=quantidade_compra_dinamica
        )
        preco_compra = 0
        for fill in order['fills']: # Calcula o preço médio da compra
            preco_compra += float(fill['price']) * float(fill['qty'])
        if order['fills']:
            preco_compra /= float(order['executedQty'])


        logging.info(f"COMPROU {quantidade_compra_dinamica:.8f} {ativo_operado} - Preço de mercado. Order ID: {order['orderId']} - Preço médio de compra: {preco_compra:.4f}")
        return order['orderId'], preco_compra # Retorna order_id e preco_compra para SL/TP
    except (BinanceOrderException, BinanceOrderMinAmountException, BinanceAPIException, Exception) as e:
        logging.error(f"Erro ao COMPRAR {ativo_operado}: {e}")
        return None, None # Retorna None em caso de falha na compra


def logica_venda(cliente_binance, codigo_ativo, ativo_operado, quantidade_venda, step_size):
    """Executa a lógica de venda."""
    quantidade_venda = float(format(quantidade_venda - (quantidade_venda % step_size), '.8f'))
    if quantidade_venda <= 0:
        logging.info("Não há quantidade suficiente para vender (após step size).")
        return True # Retorna True como se a venda tivesse sido 'bem-sucedida' (nada a vender)
    try:
        order = cliente_binance.create_order(
            symbol=codigo_ativo, side=SIDE_SELL, type=ORDER_TYPE_MARKET, quantity=quantidade_venda
        )
        logging.info(f"VENDEU {quantidade_venda:.8f} {ativo_operado} - Preço de mercado. Order ID: {order['orderId']}")
        return False # Retorna False para indicar que a posição foi fechada
    except (BinanceOrderException, BinanceOrderMinAmountException, BinanceAPIException, Exception) as e:
        logging.error(f"Erro ao VENDER {ativo_operado}: {e}")
        return True # Retorna True para manter a posição em caso de falha na venda


def estrategia_trade(dados, codigo_ativo, ativo_operado, posicao, step_size, saldos, cliente_binance, preco_compra, stop_loss_percentual, take_profit_percentual, periodo_media_rapida, periodo_media_devagar):
    """Executa a estratégia de médias móveis (refatorada com funções menores) com Stop Loss e Take Profit."""
    if dados is None:
        return posicao, preco_compra

    dados["media_rapida"] = dados["fechamento"].ewm(span=periodo_media_rapida, adjust=False).mean()
    dados["media_devagar"] = dados["fechamento"].ewm(span=periodo_media_devagar, adjust=False).mean()

    ultima_media_rapida = dados["media_rapida"].iloc[-1]
    ultima_media_devagar = dados["media_devagar"].iloc[-1]
    preco_atual = dados["fechamento"].iloc[-1] # Preço de fechamento do candle atual

    logging.info(f"Última Média Rápida: {ultima_media_rapida:.4f} | Última Média Devagar: {ultima_media_devagar:.4f} | Preço Atual: {preco_atual:.4f}")


    quantidade_atual = saldos.get(ativo_operado, 0)

    # === Lógica de Stop Loss e Take Profit ===
    if posicao and preco_compra is not None:
        stop_loss_price = preco_compra * (1 - stop_loss_percentual)
        take_profit_price = preco_compra * (1 + take_profit_percentual)

        if preco_atual <= stop_loss_price:
            logging.info(f"STOP LOSS ATIVADO! Preço de compra: {preco_compra:.4f}, Stop Loss: {stop_loss_price:.4f}, Preço atual: {preco_atual:.4f}")
            posicao = logica_venda(cliente_binance, codigo_ativo, ativo_operado, quantidade_atual, step_size)
            preco_compra = None # Reseta preco_compra ao vender
            return posicao, preco_compra # Retorna imediatamente após SL

        elif preco_atual >= take_profit_price:
            logging.info(f"TAKE PROFIT ATIVADO! Preço de compra: {preco_compra:.4f}, Take Profit: {take_profit_price:.4f}, Preço atual: {preco_atual:.4f}")
            posicao = logica_venda(cliente_binance, codigo_ativo, ativo_operado, quantidade_atual, step_size)
            preco_compra = None # Reseta preco_compra ao vender
            return posicao, preco_compra # Retorna imediatamente após TP
    # ==========================================


    if ultima_media_rapida > ultima_media_devagar and not posicao:
        order_id, preco_de_compra = logica_compra(cliente_binance, codigo_ativo, ativo_operado, step_size, saldos, porcentagem_capital_risco) # Usa função menor para compra
        if order_id: # Verifica se a compra foi bem-sucedida
            posicao = True
            preco_compra = preco_de_compra # Atualiza preco_compra se a compra ocorreu
    elif ultima_media_rapida < ultima_media_devagar and posicao:
        posicao = logica_venda(cliente_binance, codigo_ativo, ativo_operado, quantidade_atual, step_size) # Usa função menor para venda
        preco_compra = None # Reseta preco_compra ao vender


    return posicao, preco_compra # Retorna posicao e preco_compra atualizados


posicao_atual = False
preco_compra_atual = None # Variável para rastrear o preço de compra

while True:
    dados_atualizados = pegando_dados(codigo=codigo_operado, intervalo=periodo_candle)
    if dados_atualizados is not None: # Só executa a estratégia se os dados foram obtidos
        posicao_atual, preco_compra_atual = estrategia_trade(
            dados_atualizados, codigo_operado, ativo_operado, posicao_atual, step_size, saldos, cliente_binance,
            preco_compra_atual, stop_loss_percentual, take_profit_percentual, periodo_media_rapida, periodo_media_devagar
        )

    # Recarrega o saldo da conta a cada iteração para ter informações atualizadas (importante para a lógica de venda)
    try:
        conta = cliente_binance.get_account()
        saldos = {ativo["asset"]: float(ativo["free"]) for ativo in conta["balances"]}
    except BinanceAPIException as e:
        logging.error(f"Erro ao atualizar informações da conta: {e}")


    # Calcula o tempo até o próximo candle
    if dados_atualizados is not None and not dados_atualizados.empty: # Garante que dados_atualizados não é None e nem DataFrame vazio
        proximo_candle = dados_atualizados["tempo_fechamento"].iloc[-1] + pd.Timedelta(minutes=15)
        espera = (proximo_candle - pd.Timestamp.now(tz="America/Sao_Paulo")).total_seconds()
        espera = max(0, espera)  # Evita valores negativos
        logging.info(f"Aguardando {espera:.0f} segundos até o próximo candle...") # Adicionado log de espera
        time.sleep(espera)
    else:
        logging.warning("Dados não foram atualizados corretamente. Esperando 30 segundos e tentando novamente.")
        time.sleep(30) # Espera um tempo menor e tenta novamente