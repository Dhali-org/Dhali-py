import asyncio
import json
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

@dataclass
class AssetUpdates:
    name: Optional[str] = None
    earning_rate: Optional[float] = None
    earning_type: Optional[str] = None
    url: Optional[str] = None
    headers: Optional[Dict[str, str]] = field(default_factory=dict)
    docs: Optional[str] = None
    max_surcharge: Optional[float] = None
    asset_pricing_rate: Optional[float] = None
    asset_pricing_max_surcharge: Optional[float] = None
    asset_pricing_currency: Optional[Dict[str, Any]] = field(default_factory=dict)

    def to_gateway_format(self) -> Dict[str, Any]:
        gateway_updates: Dict[str, Any] = {}
        if self.name is not None:
            gateway_updates["name"] = self.name
        if self.earning_rate is not None:
            gateway_updates["asset_earning_rate"] = self.earning_rate
        if self.earning_type is not None:
            gateway_updates["asset_earning_type"] = self.earning_type
        if self.docs is not None:
            gateway_updates["docs"] = self.docs
        if self.max_surcharge is not None:
            gateway_updates["asset_earning_max_surcharge"] = self.max_surcharge
        if self.asset_pricing_rate is not None:
            gateway_updates["asset_pricing_rate"] = self.asset_pricing_rate
        if self.asset_pricing_max_surcharge is not None:
            gateway_updates["asset_pricing_max_surcharge"] = self.asset_pricing_max_surcharge
        if self.asset_pricing_currency:
            gateway_updates["asset_pricing_currency"] = self.asset_pricing_currency

        if self.url is not None or self.headers:
            gateway_updates["api_credentials"] = {}
            if self.url is not None:
                gateway_updates["api_credentials"]["url"] = self.url
            if self.headers:
                gateway_updates["api_credentials"].update(self.headers)
        
        return gateway_updates
