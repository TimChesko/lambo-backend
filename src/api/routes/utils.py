from fastapi import APIRouter
from pydantic import BaseModel
from src.utils.ton_address import address_to_raw, address_to_friendly, normalize_address

router = APIRouter()


class AddressConvertRequest(BaseModel):
    address: str


class AddressConvertResponse(BaseModel):
    original: str
    raw: str
    friendly_bounceable: str
    friendly_non_bounceable: str
    normalized: str


@router.post("/convert-address", response_model=AddressConvertResponse)
async def convert_address(request: AddressConvertRequest):
    address = request.address
    
    return AddressConvertResponse(
        original=address,
        raw=address_to_raw(address),
        friendly_bounceable=address_to_friendly(address_to_raw(address), bounceable=True),
        friendly_non_bounceable=address_to_friendly(address_to_raw(address), bounceable=False),
        normalized=normalize_address(address)
    )

