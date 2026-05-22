import { useState, useEffect, useRef } from 'react';
import { toast } from 'sonner';
import { base44 } from '@/api/base44Client';
import PriceTicker from '../components/trading/PriceTicker';
import SignalCard from '../components/trading/SignalCard';
import MetricTile from '../components/trading/MetricTile';
import IndicatorChart from '../components/trading/IndicatorChart';
import SignalHistory from '../components/trading/SignalHistory';
import { Activity, Wifi, WifiOff } from 'lucide-react';
import PullToRefresh from '../components/PullToRefresh';
import { format } from 'date-fns';

const TIMEFRAMES = ['30m', '1h'];
const BACKEND_WS_URL = "wss://xauusdpro.onrender.com/ws";

export default function Dashboard() {
  const [selectedTF, setSelectedTF] = useState('30m');
  const [isLive, setIsLive] = useState(true);
  const [wsConnected, setWsConnected] = useState(false);
  const alertConfigRef = useRef(null);
  const lastAlertFiredRef = useRef({});

  // Clean, unified core UI state initialized with standard default shapes
  const [liveMetrics, setLiveMetrics] = useState({
    price: 0,
    prevPrice: 0,
    signal: 'WAIT',
    reason: 'Establishing connection to Render API layer...',
    rsi: 50,
    emaFast: 0,
    emaSlow: 0,
    take_profit: 0,
    stop_loss: 0,
    history: [],
    indicatorHistory: []
  });

  // Load backend baseline layout alert structures
  useEffect(() => {
    base44.entities.AlertConfig.list().then(list => {
      if (list.length > 0) alertConfigRef.current = list[0];
    });
  }, []);

  // Request browser local desktop alert permissions
  useEffect(() => {
    if (typeof Notification !== 'undefined' && Notification.permission === 'default') {
      Notification.requestPermission();
    }
  }, []);

  // Main background operational loop managing the Render connection pipeline
  useEffect(() => {
    if (!isLive) {
      setWsConnected(false);
      return;
    }

    let ws = null;
    let reconnectTimeout = null;

    function connect() {
      console.log("Connecting to data stream:", BACKEND_WS_URL);
      ws = new WebSocket(BACKEND_WS_URL);

      ws.onopen = () => {
        console.log("WebSocket Connection Verified Live.");
        setWsConnected(true);
      };

      ws.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data);
          
          # Pull raw numbers directly out of your python server's payload package
          const livePrice = payload.price; 
          
          setLiveMetrics(prev => {
            // Generate basic moving technicals matching your backend baseline target models
            const nextEmaFast = prev.emaFast === 0 ? livePrice : round(prev.emaFast * 0.9 + livePrice * 0.1, 2);
            const nextEmaSlow = prev.emaSlow === 0 ? livePrice : round(prev.emaSlow * 0.95 + livePrice * 0.05, 2);
            
            // Basic functional signal rules built right into the ingestion loop
            let activeSignal = 'HOLD';
            let activeReason = 'Market structure stable within bounds.';
            if (nextEmaFast > nextEmaSlow + 0.5) {
              activeSignal = 'BUY';
              activeReason = 'Fast EMA crossed cleanly above Slow EMA trend-line.';
            } else if (nextEmaFast < nextEmaSlow - 0.5) {
              activeSignal = 'SELL';
              activeReason = 'Fast EMA dropped cleanly below Slow EMA trend-line.';
            }

            const tp = activeSignal === 'BUY' ? round(livePrice + 12, 2) : activeSignal === 'SELL' ? round(livePrice - 12, 2) : 0;
            const sl = activeSignal === 'BUY' ? round(livePrice - 6, 2) : activeSignal === 'SELL' ? round(livePrice + 6, 2) : 0;

            const newPoint = {
              time: format(new Date(), 'HH:mm:ss'),
              price: livePrice,
              emaFast: nextEmaFast,
              emaSlow: nextEmaSlow,
              rsi: prev.rsi
            };

            const updatedHistory = (activeSignal !== 'HOLD') 
              ? [...prev.history.slice(-19), { signal: activeSignal, entry: livePrice, reason: activeReason, time: Date.now() }]
              : prev.history;

            return {
              ...prev,
              price: livePrice,
              prevPrice: prev.price || livePrice,
              signal: activeSignal,
              reason: activeReason,
              emaFast: nextEmaFast,
              emaSlow: nextEmaSlow,
              take_profit: tp,
              stop_loss: sl,
              history: updatedHistory,
              indicatorHistory: [...prev.indicatorHistory.slice(-29), newPoint]
            };
          });

          // Check alerts threshold configuration triggers
          const cfg = alertConfigRef.current;
          if (cfg && cfg.price_alert_enabled && cfg.price_target) {
            const triggered = cfg.price_direction === 'above' ? livePrice >= cfg.price_target : livePrice <= cfg.price_target;
            if (triggered && lastAlertFiredRef.current['price'] !== cfg.price_target) {
              lastAlertFiredRef.current['price'] = cfg.price_target;
              toast(`🔔 Price Alert`, {
                description: `XAUUSD Target reached: $${livePrice}`,
                duration: 5000,
              });
            }
          }
        } catch (err) {
          console.error("Error reading socket metrics message packet:", err);
        }
      };

      ws.onclose = () => {
        console.log("Socket connection dropped. Re-attempting pipeline hook in 3s...");
        setWsConnected(false);
        reconnectTimeout = setTimeout(connect, 3000);
      };

      ws.onerror = (err) => {
        console.error("Pipeline network failure structural block:", err);
        ws.close();
      };
    }

    connect();

    return () => {
      if (ws) ws.close();
      if (reconnectTimeout) clearTimeout(reconnectTimeout);
    };
  }, [isLive]);

  function round(value, decimals) {
    return Number(Math.round(value + 'e' + decimals) + 'e-' + decimals);
  }

  function handleRefresh(done) {
    setLiveMetrics(prev => ({
      ...prev,
      history: [],
      indicatorHistory: []
    }));
    done();
  }

  return (
    <PullToRefresh onRefresh={handleRefresh}>
      <div className="min-h-screen bg-background text-foreground font-mono">
        {/* Header */}
        <header className="border-b border-border px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-amber-500/20 border border-amber-500/40 flex items-center justify-center">
              <span className="text-amber-400 font-black text-sm">Au</span>
            </div>
            <div>
              <h1 className="text-sm font-bold tracking-wider text-foreground">GOLD PREDICTOR</h1>
              <p className="text-xs text-muted-foreground">XAUUSD · Render Node Pipeline</p>
            </div>
          </div>
          <button
            onClick={() => setIsLive(v => !v)}
            className={`flex items-center gap-2 text-xs px-3 py-1.5 rounded-full border transition-all ${
              wsConnected
                ? 'bg-emerald-500/10 border-emerald-500/40 text-emerald-400'
                : 'bg-rose-500/10 border-rose-500/40 text-rose-400'
            }`}
          >
            {wsConnected ? (
              <><span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" /><Wifi size={12} /> STREAMING</>
            ) : (
              <><WifiOff size={12} /> DISCONNECTED</>
            )}
          </button>
        </header>

        {/* Timeframe Selector */}
        <div className="flex gap-2 px-6 pt-4">
          {TIMEFRAMES.map(tf => (
            <button
              key={tf}
              onClick={() => setSelectedTF(tf)}
              className={`px-4 py-1.5 rounded-full text-xs font-bold tracking-widest uppercase border transition-all ${
                selectedTF === tf
                  ? 'bg-amber-500/20 border-amber-500/50 text-amber-400'
                  : 'bg-transparent border-border text-muted-foreground'
              }`}
            >
              {tf}
            </button>
          ))}
        </div>

        <main className="max-w-2xl mx-auto px-4 py-6 flex flex-col gap-6">
          {/* Real-time Ticker Reading Directly From Your Python Output Array */}
          <PriceTicker price={liveMetrics.price} prevPrice={liveMetrics.prevPrice} />

          {/* Dynamic History Metric Layout Panels */}
          <div className="bg-card rounded-2xl border border-border p-4">
            <IndicatorChart indicatorHistory={liveMetrics.indicatorHistory} />
          </div>

          <SignalCard
            signal={liveMetrics.signal}
            reason={liveMetrics.reason}
            rsi={liveMetrics.rsi}
            emaFast={liveMetrics.emaFast}
            emaSlow={liveMetrics.emaSlow}
          />

          {(liveMetrics.signal === 'BUY' || liveMetrics.signal === 'SELL') && (
            <div className="grid grid-cols-2 gap-3">
              <MetricTile label="Take Profit" value={`$${liveMetrics.take_profit}`} color="green" />
              <MetricTile label="Stop Loss" value={`$${liveMetrics.stop_loss}`} color="red" />
            </div>
          )}

          <div className="bg-card rounded-2xl border border-border p-4">
            <p className="text-xs text-muted-foreground mb-3 tracking-wider uppercase">
              Signal History Data Logs
            </p>
            <SignalHistory history={liveMetrics.history} />
          </div>
        </main>
      </div>
    </PullToRefresh>
  );
}
