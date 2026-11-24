"""FIFO conservation of purchased stock and already realized margin.

Unknown/pre-existing and solar stock never subsidize grid-import profits.
Only observed execution (not a future projection) is carried across replans.
"""
from datetime import datetime, timedelta


# Quantized SOC may lag execution. Keep only recent attribution evidence, never
# forecast debt: expiry affects attribution, not purchased lots or realized credit.
_UNCONFIRMED_WINDOW = timedelta(hours=1)
_MAX_UNCONFIRMED_ACTIONS = 360


class MarginStock:
    def __init__(self, energy, reserve, lots=None, credit=0.):
        self.reserve = reserve
        self.energy = energy
        self.lots = [lot[:] for lot in lots if lot[0] > 1e-9] if lots is not None else (
            [[energy-reserve, None]] if energy-reserve > 1e-9 else [])
        self.credit = credit
        self.pending_actions = []

    def charge(self, amount, cost):
        held = min(amount, max(0., self.reserve-self.energy))
        self.energy += amount
        if amount-held > 1e-9:
            self.lots.append([amount-held, cost])

    def available(self, price, minimum, future=(), requested=None):
        """Largest FIFO prefix retaining a funded, reachable weighted cycle.

        Earlier low-price household use may borrow from a later peak, but its
        debt survives replanning and only remaining purchased stock may repay
        it. Solar and old stock never create grid-margin credit.
        """
        limit = max(0., self.energy-self.reserve)
        if requested is not None:
            limit = min(limit, max(0., requested))

        def feasible(amount):
            stock = MarginStock(self.energy, self.reserve, self.lots, self.credit)
            stock.discharge(amount, price, minimum)
            if stock.credit >= -1e-9:
                return True
            for sale, capacity in sorted(future, reverse=True):
                if sale <= 0 or not stock.lots:
                    continue
                remaining = capacity
                while remaining > 1e-9 and stock.lots:
                    quantity, cost = stock.lots[0]
                    used = min(quantity, remaining)
                    # A costly FIFO prefix may be funded by cheaper purchased
                    # lots behind it. Check cumulative margin, not each lot.
                    stock.discharge(used, sale, minimum)
                    remaining -= used
                    if stock.credit >= -1e-9:
                        return True
            return False

        if feasible(limit):
            return limit
        if feasible(0.):
            low, high = 0., limit
            for _ in range(36):
                middle = (low+high)/2
                if feasible(middle):
                    low = middle
                else:
                    high = middle
            return low
        # A changed forecast must not block profitable recovery of earlier debt.
        amount = 0.
        for quantity, cost in self.lots:
            if cost is not None and price-cost-minimum < -1e-9:
                break
            amount += quantity
        return min(amount, limit)

    def discharge(self, amount, price, minimum):
        remaining = amount
        while remaining > 1e-9 and self.lots:
            quantity, cost = self.lots[0]
            used = min(quantity, remaining)
            if cost is not None:
                self.credit += used*(price-cost-minimum)
            self.lots[0][0] -= used
            remaining -= used
            if self.lots[0][0] <= 1e-9:
                self.lots.pop(0)
        self.energy -= amount


def observed_stock(previous, now, energy, reserve, minimum):
    """Replay only elapsed commands, then reconcile against measured stock.

    A mismatch cannot mint profit credit. Unexplained energy is conservatively
    valued at the highest known acquisition cost rather than marked as free PV.
    """
    if not previous or previous['at'] > now.isoformat() or previous['reserve'] < reserve:
        return MarginStock(energy, reserve)
    stock = MarginStock(previous['energy'], previous['reserve'], previous['lots'], previous['credit'])
    at = datetime.fromisoformat(previous['at'])
    evidence = []
    # Previously clipped intervals retain their historical prices even when
    # the coordinator replaces the future action plan on every refresh.
    for action in previous.get('pending_actions', []):
        start = max(now - _UNCONFIRMED_WINDOW, datetime.fromisoformat(action['start']))
        end = min(at, datetime.fromisoformat(action['end']))
        if end > start:
            evidence.append({**action, 'start': start.isoformat(), 'end': end.isoformat()})
    for action in previous['actions']:
        start = max(at, datetime.fromisoformat(action['start']))
        end = min(now, datetime.fromisoformat(action['end']))
        if end > start and action['mode'].startswith(('laden', 'ontladen')):
            evidence.append({**action, 'start': start.isoformat(), 'end': end.isoformat()})
    elapsed = [(a, (datetime.fromisoformat(a['end']) - datetime.fromisoformat(a['start'])).total_seconds()/3600)
               for a in evidence]
    # Commands and demand forecasts are not energy meters. In particular, a
    # stationary/quantized SOC must not repeatedly sell and rebuy the same
    # stock, accumulating invented debt at every coordinator refresh. With
    # SOC alone, only the net observed change is evidence of execution.
    delta = energy - previous['energy']
    if abs(delta) <= 1e-9:
        delta = 0.
        for action in evidence[-_MAX_UNCONFIRMED_ACTIONS:]:
            start = max(now - _UNCONFIRMED_WINDOW, datetime.fromisoformat(action['start']))
            if start < datetime.fromisoformat(action['end']):
                stock.pending_actions.append({**action, 'start': start.isoformat()})
    # A measured change consumes the evidence once, including any excess or
    # opposite-direction forecast. Never carry forecast debt past confirmation.
    predicted_charge = sum((a['solar_kw'] + a['grid_kw']) * h for a, h in elapsed
                           if a['mode'].startswith('laden'))
    predicted_discharge = sum(a['power'] * h for a, h in elapsed
                              if a['mode'].startswith('ontladen'))
    charge_scale = min(1., max(0., delta) / predicted_charge) if predicted_charge > 0 else 0.
    # Forecast power is not a cap on measured solar production. If the only
    # elapsed source commands were solar-only, all confirmed gain has that
    # source even when sunshine or quantized SOC exceeds the forecast. Mixed
    # or unknown execution remains conservative and cannot acquire free stock.
    solar_only = bool(elapsed) and all(
        a['mode'] == 'laden_met_zonne_energie' and a['grid_kw'] == 0
        for a, _ in elapsed)
    if solar_only and predicted_charge > 0:
        charge_scale = max(0., delta) / predicted_charge
    elif solar_only and delta > 0:
        stock.charge(delta, None)
    discharge_scale = min(1., max(0., -delta) / predicted_discharge) if predicted_discharge > 0 else 0.
    for action, hours in elapsed:
        if action['mode'].startswith('laden'):
            stock.charge(action['solar_kw'] * hours * charge_scale, None)
            stock.charge(action['grid_kw'] * hours * charge_scale, action['import_price'])
        elif action['mode'].startswith('ontladen'):
            stock.discharge(action['power'] * hours * discharge_scale,
                            action['sale_price'], minimum)
    if stock.reserve > reserve:
        # Full-cycle accounting exposes stock previously hidden by the reserve.
        # Retain purchased FIFO lots and realized debt; a policy migration is
        # not a new free battery. Price untracked stock conservatively.
        costs = [cost for _, cost in stock.lots if cost is not None]
        costs += [a['import_price'] for a in previous['actions'] if a['grid_kw'] > 0]
        exposed = max(0., stock.energy-reserve) - sum(q for q, _ in stock.lots)
        if exposed > 1e-9:
            stock.lots.insert(0, [exposed, max(costs) if costs else None])
        stock.reserve = reserve
    difference = energy-stock.energy
    if abs(difference) > 1e-6:
        stock.credit = min(0., stock.credit)
        costs = [cost for _, cost in stock.lots if cost is not None]
        costs += [a['import_price'] for a in previous['actions'] if a['grid_kw'] > 0]
        if difference > 0:
            stock.charge(difference, max(costs) if costs else None)
        else:
            # Remove unobserved consumption from FIFO stock, but do not invent
            # a zero-price sale or realized debt without an executed command.
            credit = stock.credit
            stock.discharge(-difference, 0., minimum)
            stock.credit = credit
    stock.energy = energy
    return stock


def charge_stock(stock, slots, start, end, mode, max_charge):
    for action in execution_actions([dict(start=start.isoformat(), end=end.isoformat(), mode=mode)],
                                    slots, max_charge, 0.):
        hours = (datetime.fromisoformat(action['end'])-datetime.fromisoformat(action['start'])).total_seconds()/3600
        stock.charge(action['solar_kw']*hours, None)
        stock.charge(action['grid_kw']*hours, action['import_price'])


def execution_actions(windows, slots, max_charge, max_discharge):
    actions = []
    for window in windows:
        # Standby does not establish a sale or a completed discharge phase.
        # Reconciliation still observes any corresponding drop in SOC.
        if window.get('standby'):
            continue
        for slot in slots:
            start = max(datetime.fromisoformat(window['start']), slot['start'])
            end = min(datetime.fromisoformat(window['end']), slot['end'])
            if end <= start:
                continue
            mode = window['mode']
            net = float(slot['net_solar_kwh'])/float(slot['hours'])
            solar = min(max_charge, max(0., net)) if mode.startswith('laden') else 0.
            grid = max_charge-solar if mode == 'laden_van_net' else 0.
            home = min(max_discharge, max(0., -net))
            power = max_discharge if mode == 'ontladen_naar_net' else home
            sale = ((home*slot['import_price']+(power-home)*slot['export_price'])/power) if power else 0.
            actions.append(dict(start=start.isoformat(), end=end.isoformat(), mode=mode,
                solar_kw=solar, grid_kw=grid, power=power, sale_price=sale,
                import_price=slot['import_price']))
    return sorted(actions, key=lambda a: a['start'])
