# main.py
from fastapi import FastAPI, status, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Annotated
from database import get_async_session, init_db
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import update, delete


from contextlib import asynccontextmanager

from sqlalchemy import Column, Integer, String, Boolean, DateTime, func
from database import Base

class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, index=True)
    description = Column(String(255))
    price = Column(Integer)
    on_offer = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    modified_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    is_deleted = Column(Boolean, default=False, nullable=False)

    def __repr__(self):
        return f"<Item name={self.name} price={self.price}>"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Run before the application starts
    await init_db()
    yield
    # Shutdown: Run when the application is shutting down
    # Clean-up code can go here

app = FastAPI(lifespan=lifespan)

# Pydantic models (schemas)
class ItemSchema(BaseModel):
    id: int = None
    name: str
    description: str
    price: int
    on_offer: bool

    class Config:
        from_attributes = True

@app.get("/items", response_model=List[ItemSchema], status_code=200)
async def get_all_items(db: AsyncSession = Depends(get_async_session)):
    result = await db.execute(select(Item))
    items = result.scalars().all()
    return items

@app.get("/item/{item_id}", response_model=ItemSchema, status_code=status.HTTP_200_OK)
async def get_an_item(item_id: int, db: AsyncSession = Depends(get_async_session)):
    result = await db.execute(select(Item).where(Item.id == item_id))
    item = result.scalar_one_or_none()
    if item is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return item

@app.post("/items", response_model=ItemSchema, status_code=status.HTTP_201_CREATED)
async def create_an_item(item: ItemSchema, db: AsyncSession = Depends(get_async_session)):
    # Check if item exists
    result = await db.execute(select(Item).where(Item.name == item.name))
    existing_item = result.scalar_one_or_none()
    if existing_item is not None:
        raise HTTPException(status_code=400, detail="Item already exists")

    # Create new item
    new_item = Item(
        name=item.name,
        price=item.price,
        description=item.description,
        on_offer=item.on_offer,
    )

    db.add(new_item)
    await db.commit()
    await db.refresh(new_item)
    return new_item

@app.put("/item/{item_id}", response_model=ItemSchema, status_code=status.HTTP_200_OK)
async def update_an_item(item_id: int, item: ItemSchema, db: AsyncSession = Depends(get_async_session)):
    # Get the item to update
    result = await db.execute(select(Item).where(Item.id == item_id))
    item_to_update = result.scalar_one_or_none()
    
    if item_to_update is None:
        raise HTTPException(status_code=404, detail="Item not found")
    
    # Update the item attributes
    item_to_update.name = item.name
    item_to_update.price = item.price
    item_to_update.description = item.description
    item_to_update.on_offer = item.on_offer
    
    await db.commit()
    await db.refresh(item_to_update)
    return item_to_update

@app.delete("/item/{item_id}", response_model=ItemSchema)
async def delete_item(item_id: int, db: AsyncSession = Depends(get_async_session)):
    # Get the item to delete
    result = await db.execute(select(Item).where(Item.id == item_id))
    item_to_delete = result.scalar_one_or_none()
    
    if item_to_delete is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Resource Not Found"
        )
    
    await db.delete(item_to_delete)
    await db.commit()
    
    return item_to_delete