import logging
from decimal import Decimal
from typing import Dict, List

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.data_feed.candles_feed.candles_factory import CandlesFactory, CandlesConfig
from hummingbot.connector.connector_base import ConnectorBase

class PMMCandles(ScriptStrategyBase):
    """
     Simple PMM with added Risk Managements
    - Risk management with position sizing
    """
    bid_spread = Decimal('0.0001')
    ask_spread = Decimal('0.0001')
    order_refresh_time = 15
    base_order_amount = Decimal('0.01')
    create_timestamp = 0
    trading_pair = "ETH-USDT"
    exchange = "binance_paper_trade"
    price_source = PriceType.MidPrice

    # Candles configuration
    candle_exchange = "binance"
    candles_interval = "1m"
    candles_length = 30
    max_records = 1000

    candles = CandlesFactory.get_candle(
        CandlesConfig(
            connector=candle_exchange,
            trading_pair=trading_pair,
            interval=candles_interval,
            max_records=max_records
        )
    )

    markets = {exchange: {trading_pair}}

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        self.candles.start()
        self.min_spread = Decimal('0.0001')
        self.max_spread = Decimal('0.0010')
        self.total_profit = Decimal('0')
        self.logger().info("Strategy initialized")

    def on_stop(self):
        self.candles.stop()
        self.logger().info("Strategy stopped")

    def on_tick(self):
        if self.create_timestamp <= self.current_timestamp:
            self.cancel_all_orders()
            proposal = self.create_proposal()
            if proposal:
                proposal_adjusted = self.adjust_proposal_to_budget(proposal)
                if proposal_adjusted:
                    self.place_orders(proposal_adjusted)
            self.create_timestamp = self.order_refresh_time + self.current_timestamp

    def get_candles_with_features(self):
        if not self.candles.ready:
            return None
        candles_df = self.candles.candles_df
        candles_df.ta.rsi(length=self.candles_length, append=True)
        candles_df.ta.sma(length=self.candles_length, append=True)
        return candles_df

    def create_proposal(self) -> List[OrderCandidate]:
        if not self.candles.ready:
            self.logger().warning("Candles not ready")
            return []

        try:
            ref_price = self.connectors[self.exchange].get_price_by_type(self.trading_pair, self.price_source)
            if not ref_price:
                self.logger().warning("No reference price")
                return []

            # Get available balance
            quote_asset = self.trading_pair.split("-")[1]
            base_asset = self.trading_pair.split("-")[0]
            quote_balance = self.connectors[self.exchange].get_available_balance(quote_asset)
            base_balance = self.connectors[self.exchange].get_available_balance(base_asset)

            # Calculates dynamic spread based on recent volatility
            candles_df = self.candles.candles_df
            recent_high = Decimal(str(candles_df["high"].iloc[-1]))
            recent_low = Decimal(str(candles_df["low"].iloc[-1]))
            recent_range = (recent_high - recent_low) / Decimal(str(ref_price))
            dynamic_spread = min(self.max_spread, max(self.min_spread, recent_range * Decimal('0.5')))

            # Calculates order amounts (1% of available balance or base amount)
            buy_amount = min(
                self.base_order_amount,
                (Decimal(str(quote_balance)) * Decimal('0.01')) / Decimal(str(ref_price))
            )
            sell_amount = min(
                self.base_order_amount,
                Decimal(str(base_balance)) * Decimal('0.01')
            )

            # Calculates prices
            buy_price = Decimal(str(ref_price)) * (Decimal('1') - dynamic_spread)
            sell_price = Decimal(str(ref_price)) * (Decimal('1') + dynamic_spread)

            # Creates orders
            buy_order = OrderCandidate(
                trading_pair=self.trading_pair,
                is_maker=True,
                order_type=OrderType.LIMIT,
                order_side=TradeType.BUY,
                amount=buy_amount,
                price=buy_price
            )

            sell_order = OrderCandidate(
                trading_pair=self.trading_pair,
                is_maker=True,
                order_type=OrderType.LIMIT,
                order_side=TradeType.SELL,
                amount=sell_amount,
                price=sell_price
            )

            return [buy_order, sell_order]
        # proposals exception handling 
        except Exception as e:
            self.logger().error(f"Error creating proposal: {e}", exc_info=True)
            return []

    def adjust_proposal_to_budget(self, proposal: List[OrderCandidate]) -> List[OrderCandidate]:
        try:
            return self.connectors[self.exchange].budget_checker.adjust_candidates(proposal, all_or_none=True)
        except Exception as e:
            self.logger().error(f"Budget adjustment failed: {e}", exc_info=True)
            return []

    def place_orders(self, proposal: List[OrderCandidate]) -> None:
        for order in proposal:
            if self.calculate_expected_profit(order) > Decimal('0'):
                self.place_order(connector_name=self.exchange, order=order)

    def calculate_expected_profit(self, order: OrderCandidate) -> Decimal:
        try:
            mid_price = Decimal(str(self.connectors[self.exchange].get_price_by_type(
                self.trading_pair, PriceType.MidPrice
            )))
            if order.order_side == TradeType.SELL:
                return Decimal(str(order.price)) - mid_price
            else:
                return mid_price - Decimal(str(order.price))
        except Exception as e:
            self.logger().error(f"Profit calculation failed: {e}", exc_info=True)
            return Decimal('0')

    def place_order(self, connector_name: str, order: OrderCandidate):
        try:
            if order.order_side == TradeType.BUY:
                self.buy(
                    connector_name=connector_name,
                    trading_pair=order.trading_pair,
                    amount=order.amount,
                    order_type=order.order_type,
                    price=order.price
                )
                self.logger().info(f"Placed BUY order: {order.amount} at {order.price}")
            else:
                self.sell(
                    connector_name=connector_name,
                    trading_pair=order.trading_pair,
                    amount=order.amount,
                    order_type=order.order_type,
                    price=order.price
                )
                self.logger().info(f"Placed SELL order: {order.amount} at {order.price}")
        except Exception as e:
            self.logger().error(f"Order placement failed: {e}", exc_info=True)

    def cancel_all_orders(self):
        try:
            for order in self.get_active_orders(connector_name=self.exchange):
                self.cancel(self.exchange, order.trading_pair, order.client_order_id)
        except Exception as e:
            self.logger().error(f"Order cancellation failed: {e}", exc_info=True)

    def did_fill_order(self, event: OrderFilledEvent):
        msg = f"{event.trade_type.name} {round(event.amount, 2)} {event.trading_pair} at {round(event.price, 2)}"
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)
        
        try:
            if event.trade_type == TradeType.SELL:
                self.total_profit += Decimal(str(event.amount)) * Decimal(str(event.price))
            else:
                self.total_profit -= Decimal(str(event.amount)) * Decimal(str(event.price))
        except Exception as e:
            self.logger().error(f"Profit tracking failed: {e}", exc_info=True)

    def format_status(self) -> str:
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        
        lines = []
        
        # Balances
        try:
            balance_df = self.get_balance_df()
            lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])
        except Exception:
            lines.extend(["", "  Error getting balance information"])

        # Orders
        try:
            df = self.active_orders_df()
            lines.extend(["", "  Orders:"] + ["    " + line for line in df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active orders"])
        except Exception:
            lines.extend(["", "  Error getting order information"])

        # stats for Profits
        lines.extend([f"\n  Total Profit: {float(self.total_profit):.4f} {self.trading_pair.split('-')[1]}"])

        # Candles
        lines.extend(["\n----------------------------------------------------------------------"])
        try:
            candles_df = self.get_candles_with_features()
            if candles_df is not None:
                lines.extend([f"  Candles: {self.candles.name} | Interval: {self.candles.interval}", ""])
                lines.extend(["    " + line for line in candles_df.tail(self.candles_length).iloc[::-1].to_string(index=False).split("\n")])
            else:
                lines.extend(["  Candles data not ready yet"])
        except Exception:
            lines.extend(["  Error getting candles information"])

        return "\n".join(lines)
