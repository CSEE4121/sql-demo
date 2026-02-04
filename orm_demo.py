"""
ORM DEMONSTRATION: Good and Bad Practices with SQLAlchemy
PostgreSQL Edition
=========================================================

This file demonstrates ORM usage patterns, including:
- Proper ORM setup and model definitions
- Good query patterns
- BAD query patterns (with explanations of why they're bad)
- How to identify and fix N+1 query problems
- Eager loading vs lazy loading
- Raw SQL when ORM is insufficient

Requirements:
    pip install sqlalchemy psycopg2-binary

Usage:
    # First, create the PostgreSQL database:
    createdb demo_db

    # Then run the demo:
    python orm_demo.py

    # Or specify a custom connection string:
    DATABASE_URL=postgresql://user:pass@localhost/demo_db python orm_demo.py

This will connect to PostgreSQL and run various examples.
"""

import os
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Boolean,
    ForeignKey, DateTime, Text, Numeric, CheckConstraint,
    func, text, event, select, and_, or_
)
from sqlalchemy.orm import (
    declarative_base, relationship, sessionmaker,
    joinedload, selectinload, subqueryload, lazyload,
    Session
)
from datetime import datetime, timedelta
import logging
import random

# ============================================================================
# SETUP: Configure SQLAlchemy logging to see generated SQL
# ============================================================================

# Enable SQL logging - VERY useful for debugging ORM issues!
# Set to True to see every SQL query the ORM generates
SHOW_SQL = True

if SHOW_SQL:
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# ============================================================================
# DATABASE CONNECTION CONFIGURATION
# ============================================================================

# Default PostgreSQL connection string
# Can be overridden with DATABASE_URL environment variable
DEFAULT_DATABASE_URL = 'postgresql://localhost/demo_db'

def get_database_url():
    """Get database URL from environment or use default."""
    return os.environ.get('DATABASE_URL', DEFAULT_DATABASE_URL)

# ============================================================================
# PART 1: MODEL DEFINITIONS
# ============================================================================

Base = declarative_base()


class Manufacturer(Base):
    """
    Represents a product manufacturer.

    Relationships:
        - One-to-Many with Product (one manufacturer has many products)
    """
    __tablename__ = 'manufacturers'

    manufacturer_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    country = Column(String(50), nullable=False)
    founded_year = Column(Integer)
    website = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationship: Access all products from this manufacturer
    # lazy='select' is the default - loads products on first access (can cause N+1!)
    products = relationship('Product', back_populates='manufacturer', lazy='select')

    def __repr__(self):
        return f"<Manufacturer(id={self.manufacturer_id}, name='{self.name}')>"


class Category(Base):
    """
    Product categories with self-referential relationship for hierarchy.
    """
    __tablename__ = 'categories'

    category_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False, unique=True)
    description = Column(Text)
    parent_category_id = Column(Integer, ForeignKey('categories.category_id'))

    # Self-referential relationship for parent/child categories
    parent = relationship('Category', remote_side=[category_id], backref='children')
    products = relationship('Product', back_populates='category')

    def __repr__(self):
        return f"<Category(id={self.category_id}, name='{self.name}')>"


class Product(Base):
    """
    Core product entity with relationships to Manufacturer and Category.
    """
    __tablename__ = 'products'

    product_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    sku = Column(String(50), unique=True, nullable=False)
    description = Column(Text)
    price = Column(Numeric(10, 2), nullable=False)
    quantity_in_stock = Column(Integer, nullable=False, default=0)
    manufacturer_id = Column(Integer, ForeignKey('manufacturers.manufacturer_id'))
    category_id = Column(Integer, ForeignKey('categories.category_id'))
    weight_kg = Column(Numeric(8, 3))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    manufacturer = relationship('Manufacturer', back_populates='products')
    category = relationship('Category', back_populates='products')
    order_items = relationship('OrderItem', back_populates='product')

    __table_args__ = (
        CheckConstraint('price >= 0', name='check_price_positive'),
        CheckConstraint('quantity_in_stock >= 0', name='check_quantity_positive'),
    )

    def __repr__(self):
        return f"<Product(id={self.product_id}, name='{self.name}', price={self.price})>"


class Order(Base):
    """
    Customer order header.
    """
    __tablename__ = 'orders'

    order_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_name = Column(String(100), nullable=False)
    customer_email = Column(String(255), nullable=False)
    shipping_address = Column(Text, nullable=False)
    order_date = Column(DateTime, default=datetime.utcnow)
    status = Column(String(20), default='pending')
    total_amount = Column(Numeric(12, 2))
    notes = Column(Text)

    # Relationship to order items
    items = relationship('OrderItem', back_populates='order', cascade='all, delete-orphan')

    def __repr__(self):
        return f"<Order(id={self.order_id}, customer='{self.customer_name}', status='{self.status}')>"


class OrderItem(Base):
    """
    Line items for orders - junction table between Order and Product.
    """
    __tablename__ = 'order_items'

    order_item_id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey('orders.order_id', ondelete='CASCADE'), nullable=False)
    product_id = Column(Integer, ForeignKey('products.product_id'), nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)
    discount_percent = Column(Numeric(5, 2), default=0)

    # Relationships
    order = relationship('Order', back_populates='items')
    product = relationship('Product', back_populates='order_items')

    @property
    def line_total(self):
        """Calculate line total with discount applied."""
        discount_multiplier = 1 - (float(self.discount_percent or 0) / 100)
        return float(self.quantity) * float(self.unit_price) * discount_multiplier

    def __repr__(self):
        return f"<OrderItem(order={self.order_id}, product={self.product_id}, qty={self.quantity})>"


# ============================================================================
# PART 2: DATABASE SETUP AND SAMPLE DATA
# ============================================================================

def create_database():
    """Create database engine and tables."""
    database_url = get_database_url()
    print(f"Connecting to: {database_url}")

    # Create PostgreSQL engine
    # pool_pre_ping=True helps handle stale connections
    engine = create_engine(
        database_url,
        echo=False,
        pool_pre_ping=True
    )

    # Drop existing tables and recreate (for clean demo runs)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    return engine


def populate_sample_data(session: Session):
    """Insert sample data into the database."""

    # Manufacturers
    manufacturers = [
        Manufacturer(name='TechCorp Industries', country='USA', founded_year=1985, website='https://techcorp.example.com'),
        Manufacturer(name='EuroElectronics GmbH', country='Germany', founded_year=1992, website='https://euroelec.example.de'),
        Manufacturer(name='AsiaManufacturing Ltd', country='Japan', founded_year=1978, website='https://asiamfg.example.jp'),
        Manufacturer(name='Nordic Components', country='Sweden', founded_year=2001, website='https://nordic.example.se'),
        Manufacturer(name='Pacific Goods Co', country='Australia', founded_year=2010, website=None),
    ]
    session.add_all(manufacturers)

    # Categories
    electronics = Category(name='Electronics', description='Electronic devices and components')
    session.add(electronics)
    session.flush()  # Get the ID

    computers = Category(name='Computers', description='Desktop and laptop computers', parent_category_id=electronics.category_id)
    peripherals = Category(name='Peripherals', description='Computer peripherals and accessories', parent_category_id=electronics.category_id)
    audio = Category(name='Audio', description='Audio equipment', parent_category_id=electronics.category_id)
    home = Category(name='Home & Garden', description='Home and garden products')
    furniture = Category(name='Furniture', description='Home and office furniture', parent_category_id=None)
    session.add_all([computers, peripherals, audio, home, furniture])
    session.flush()

    # Products
    products = [
        Product(name='Pro Laptop 15"', sku='TECH-LAP-001', price=1299.99, quantity_in_stock=45, manufacturer_id=1, category_id=computers.category_id),
        Product(name='Wireless Mouse', sku='TECH-MOU-001', price=49.99, quantity_in_stock=200, manufacturer_id=1, category_id=peripherals.category_id),
        Product(name='Mechanical Keyboard', sku='EURO-KEY-001', price=159.99, quantity_in_stock=75, manufacturer_id=2, category_id=peripherals.category_id),
        Product(name='Studio Headphones', sku='ASIA-AUD-001', price=299.99, quantity_in_stock=30, manufacturer_id=3, category_id=audio.category_id),
        Product(name='USB-C Hub', sku='ASIA-HUB-001', price=79.99, quantity_in_stock=150, manufacturer_id=3, category_id=peripherals.category_id),
        Product(name='Standing Desk', sku='NORD-DSK-001', price=599.99, quantity_in_stock=20, manufacturer_id=4, category_id=furniture.category_id),
        Product(name='Monitor Arm', sku='NORD-ARM-001', price=129.99, quantity_in_stock=60, manufacturer_id=4, category_id=peripherals.category_id),
        Product(name='Webcam 4K', sku='TECH-CAM-001', price=199.99, quantity_in_stock=85, manufacturer_id=1, category_id=peripherals.category_id),
        Product(name='Bluetooth Speaker', sku='PACI-SPK-001', price=89.99, quantity_in_stock=0, manufacturer_id=5, category_id=audio.category_id),
        Product(name='Budget Mouse', sku='PACI-MOU-001', price=9.99, quantity_in_stock=500, manufacturer_id=5, category_id=peripherals.category_id),
        # Product without manufacturer (orphan)
        Product(name='Orphan Product', sku='ORPH-001', price=19.99, quantity_in_stock=10, manufacturer_id=None, category_id=peripherals.category_id),
    ]
    session.add_all(products)
    session.flush()

    # Orders and Order Items
    orders_data = [
        ('Alice Johnson', 'alice@example.com', '123 Main St, New York', 'delivered', [
            (1, 1, 1299.99, 0), (2, 1, 49.99, 0), (3, 1, 159.99, 0)  # Laptop, Mouse, Keyboard
        ]),
        ('Bob Smith', 'bob@example.com', '456 Oak Ave, Los Angeles', 'shipped', [
            (4, 1, 299.99, 0), (5, 1, 79.99, 10)  # Headphones, USB Hub (10% discount)
        ]),
        ('Carol White', 'carol@example.com', '789 Pine Rd, Chicago', 'processing', [
            (6, 1, 599.99, 0)  # Standing Desk
        ]),
        ('David Brown', 'david@example.com', '321 Elm St, Houston', 'pending', [
            (3, 1, 159.99, 0), (10, 2, 9.99, 0)  # Keyboard, 2x Budget Mouse
        ]),
        ('Eve Davis', 'eve@example.com', '654 Maple Dr, Phoenix', 'cancelled', [
            (4, 1, 299.99, 0)  # Headphones (cancelled)
        ]),
    ]

    for customer_name, email, address, status, items in orders_data:
        order = Order(
            customer_name=customer_name,
            customer_email=email,
            shipping_address=address,
            status=status,
            order_date=datetime.now() - timedelta(days=random.randint(1, 60))
        )
        session.add(order)
        session.flush()

        total = 0
        for product_id, qty, price, discount in items:
            item = OrderItem(
                order_id=order.order_id,
                product_id=product_id,
                quantity=qty,
                unit_price=price,
                discount_percent=discount
            )
            session.add(item)
            total += qty * price * (1 - discount/100)

        order.total_amount = total

    session.commit()
    print("Sample data created successfully!")


# ============================================================================
# PART 3: GOOD ORM PATTERNS
# ============================================================================

def good_patterns_demo(session: Session):
    """Demonstrate good ORM usage patterns."""

    print("\n" + "="*70)
    print("GOOD ORM PATTERNS")
    print("="*70)

    # -------------------------------------------------------------------------
    # Good Pattern 1: Simple filtered query
    # -------------------------------------------------------------------------
    print("\n--- Good: Simple filtered query ---")
    expensive_products = session.query(Product).filter(
        Product.price > 100,
        Product.is_active == True
    ).all()
    print(f"Found {len(expensive_products)} expensive products")

    # -------------------------------------------------------------------------
    # Good Pattern 2: Eager loading with joinedload
    # This loads related data in a SINGLE query using a JOIN
    # -------------------------------------------------------------------------
    print("\n--- Good: Eager loading with joinedload ---")
    products_with_mfg = session.query(Product).options(
        joinedload(Product.manufacturer)
    ).filter(Product.price > 100).all()

    # Accessing manufacturer.name does NOT trigger additional queries!
    for p in products_with_mfg:
        mfg_name = p.manufacturer.name if p.manufacturer else "No manufacturer"
        print(f"  {p.name}: ${p.price} by {mfg_name}")

    # -------------------------------------------------------------------------
    # Good Pattern 3: selectinload for one-to-many relationships
    # Better than joinedload when loading collections (avoids row duplication)
    # -------------------------------------------------------------------------
    print("\n--- Good: selectinload for collections ---")
    manufacturers_with_products = session.query(Manufacturer).options(
        selectinload(Manufacturer.products)
    ).all()

    # This uses 2 queries: one for manufacturers, one for ALL their products
    for mfg in manufacturers_with_products:
        print(f"  {mfg.name}: {len(mfg.products)} products")

    # -------------------------------------------------------------------------
    # Good Pattern 4: Aggregation using ORM
    # -------------------------------------------------------------------------
    print("\n--- Good: Aggregation query ---")
    from sqlalchemy import func

    result = session.query(
        Manufacturer.name,
        func.count(Product.product_id).label('product_count'),
        func.avg(Product.price).label('avg_price')
    ).outerjoin(Product).group_by(Manufacturer.manufacturer_id).all()

    for name, count, avg_price in result:
        avg_str = f"${avg_price:.2f}" if avg_price else "N/A"
        print(f"  {name}: {count} products, avg price: {avg_str}")

    # -------------------------------------------------------------------------
    # Good Pattern 5: Explicit column selection (reduces memory)
    # -------------------------------------------------------------------------
    print("\n--- Good: Select only needed columns ---")
    # Instead of loading full Product objects, load only what's needed
    product_names_prices = session.query(
        Product.name,
        Product.price
    ).filter(Product.is_active == True).limit(5).all()

    for name, price in product_names_prices:
        print(f"  {name}: ${price}")

    # -------------------------------------------------------------------------
    # Good Pattern 6: Exists subquery for filtering
    # -------------------------------------------------------------------------
    print("\n--- Good: Using exists() for efficient filtering ---")
    from sqlalchemy import exists

    # Find manufacturers that have at least one product over $500
    has_expensive = exists().where(
        and_(
            Product.manufacturer_id == Manufacturer.manufacturer_id,
            Product.price > 500
        )
    )

    mfgs_with_expensive = session.query(Manufacturer).filter(has_expensive).all()
    print(f"Manufacturers with products over $500: {[m.name for m in mfgs_with_expensive]}")

    # -------------------------------------------------------------------------
    # Good Pattern 7: Bulk operations
    # -------------------------------------------------------------------------
    print("\n--- Good: Bulk update (single query) ---")
    # Update all products from a manufacturer in one query
    updated_count = session.query(Product).filter(
        Product.manufacturer_id == 1
    ).update({'quantity_in_stock': Product.quantity_in_stock + 10})
    print(f"Updated {updated_count} products in single query")
    session.rollback()  # Undo for demo purposes


# ============================================================================
# PART 4: BAD ORM PATTERNS (WITH EXPLANATIONS)
# ============================================================================

def bad_patterns_demo(session: Session):
    """
    Demonstrate BAD ORM patterns that you should AVOID.
    Each example includes detailed explanation of why it's bad.
    """

    print("\n" + "="*70)
    print("BAD ORM PATTERNS (AVOID THESE!)")
    print("="*70)

    # =========================================================================
    # BAD PATTERN 1: The N+1 Query Problem
    # =========================================================================
    print("\n--- BAD: N+1 Query Problem ---")
    print("(Watch the SQL output - you'll see MANY queries!)\n")

    # BAD CODE START --------------------------------------------------------
    # This code looks innocent but is TERRIBLE for performance!
    products = session.query(Product).limit(5).all()  # Query 1

    for product in products:
        # PROBLEM: Each access to product.manufacturer triggers a NEW query!
        # For 5 products, this runs 5 additional queries (N+1 total)
        # For 1000 products, this would run 1001 queries!
        if product.manufacturer:  # Query 2, 3, 4, 5, 6...
            print(f"  {product.name} by {product.manufacturer.name}")
    # BAD CODE END ----------------------------------------------------------

    print("""
    WHY THIS IS BAD:
    - For N products, this generates N+1 database queries
    - Each query has network latency + query parsing overhead
    - With 1000 products, you'd have 1001 round trips to the database!
    - Database connection pools can be exhausted

    HOW TO FIX:
    - Use eager loading: query(Product).options(joinedload(Product.manufacturer))
    - Or use selectinload() for collections
    """)

    # =========================================================================
    # BAD PATTERN 2: Loading entire objects when you only need a few fields
    # =========================================================================
    print("\n--- BAD: Loading unnecessary data ---")

    # BAD CODE START --------------------------------------------------------
    # We only need names and prices, but we're loading EVERYTHING
    all_product_data = session.query(Product).all()

    # Worse: We're iterating ALL products just to get a few fields
    for p in all_product_data:
        # We loaded description, weight_kg, created_at, etc. for no reason!
        name = p.name
        price = p.price
    # BAD CODE END ----------------------------------------------------------

    print("""
    WHY THIS IS BAD:
    - Transfers more data than needed over the network
    - Uses more memory on the application server
    - SQLAlchemy creates full ORM objects with all their overhead
    - Can be 10x slower than selecting just the columns you need

    HOW TO FIX:
    - Select only needed columns: query(Product.name, Product.price)
    - Use .with_entities() for more complex cases
    """)

    # =========================================================================
    # BAD PATTERN 3: Query inside a loop
    # =========================================================================
    print("\n--- BAD: Query inside a loop ---")

    # BAD CODE START --------------------------------------------------------
    manufacturer_ids = [1, 2, 3]
    all_products = []

    for mfg_id in manufacturer_ids:
        # PROBLEM: Running a separate query for each manufacturer!
        products = session.query(Product).filter(
            Product.manufacturer_id == mfg_id
        ).all()
        all_products.extend(products)
    # BAD CODE END ----------------------------------------------------------

    print("""
    WHY THIS IS BAD:
    - 3 manufacturers = 3 queries
    - 100 manufacturers = 100 queries!
    - Each query has overhead even if it returns few/no results

    HOW TO FIX:
    - Use IN clause: filter(Product.manufacturer_id.in_(manufacturer_ids))
    - Single query instead of N queries
    """)

    # =========================================================================
    # BAD PATTERN 4: Using Python for operations the database should do
    # =========================================================================
    print("\n--- BAD: Filtering/aggregating in Python instead of SQL ---")

    # BAD CODE START --------------------------------------------------------
    # Fetching ALL products to count expensive ones - TERRIBLE!
    all_products = session.query(Product).all()

    # Counting in Python - transfers ALL data just to count!
    expensive_count = len([p for p in all_products if float(p.price) > 100])
    print(f"  Expensive products: {expensive_count}")

    # Calculating average in Python - even worse for large datasets
    total_price = sum(float(p.price) for p in all_products)
    avg_price = total_price / len(all_products) if all_products else 0
    print(f"  Average price: ${avg_price:.2f}")
    # BAD CODE END ----------------------------------------------------------

    print("""
    WHY THIS IS BAD:
    - Transfers ALL rows from database to application
    - Uses application memory to hold all records
    - Python is slower than database for these operations
    - Database has optimized algorithms for COUNT, AVG, SUM, etc.
    - Network transfer of 1 million rows vs 1 number!

    HOW TO FIX:
    - Use database aggregations:
      session.query(func.count(Product.product_id)).filter(Product.price > 100).scalar()
      session.query(func.avg(Product.price)).scalar()
    """)

    # =========================================================================
    # BAD PATTERN 5: Implicit lazy loading in templates/serialization
    # =========================================================================
    print("\n--- BAD: Lazy loading during serialization ---")

    # BAD CODE START --------------------------------------------------------
    def bad_product_to_dict(product):
        """
        BAD: This function triggers lazy loads when accessing relationships!
        If called in a loop, this is an N+1 problem in disguise.
        """
        return {
            'name': product.name,
            'price': float(product.price),
            # PROBLEM: These access relationships, triggering lazy loads!
            'manufacturer': product.manufacturer.name if product.manufacturer else None,
            'category': product.category.name if product.category else None,
        }

    products = session.query(Product).limit(3).all()
    # Each to_dict() call triggers 2 lazy loads (manufacturer + category)
    # For 3 products: 1 + 3*2 = 7 queries!
    serialized = [bad_product_to_dict(p) for p in products]
    # BAD CODE END ----------------------------------------------------------

    print("""
    WHY THIS IS BAD:
    - Lazy loads hidden inside function calls are hard to spot
    - N+1 problem occurs during serialization/API response generation
    - Often happens when returning data from APIs
    - Detached objects (after session close) will raise errors

    HOW TO FIX:
    - Eager load needed relationships BEFORE passing to serialization
    - Use joinedload/selectinload in the original query
    - Or use a serialization library that's ORM-aware (like marshmallow)
    """)

    # =========================================================================
    # BAD PATTERN 6: Not using transactions properly
    # =========================================================================
    print("\n--- BAD: Improper transaction handling ---")

    # BAD CODE START --------------------------------------------------------
    def bad_create_order_items(session, order_id, items_data):
        """
        BAD: No proper transaction handling!
        If one item fails, previous items are still committed.
        """
        for item_data in items_data:
            item = OrderItem(
                order_id=order_id,
                product_id=item_data['product_id'],
                quantity=item_data['quantity'],
                unit_price=item_data['unit_price']
            )
            session.add(item)
            session.commit()  # BAD: Committing each item separately!
            # If item 3 fails, items 1 and 2 are already committed
            # This leaves the database in an inconsistent state!
    # BAD CODE END ----------------------------------------------------------

    print("""
    WHY THIS IS BAD:
    - Each commit is expensive (fsync to disk)
    - Partial failures leave data in inconsistent state
    - No way to roll back all changes if something fails
    - Violates ACID properties

    HOW TO FIX:
    - Add all items, then commit ONCE at the end
    - Use session context manager: with Session() as session:
    - Or explicit try/except with rollback
    """)

    # =========================================================================
    # BAD PATTERN 7: Holding sessions open too long
    # =========================================================================
    print("\n--- BAD: Long-lived sessions ---")

    print("""
    BAD CODE EXAMPLE (not executed):

    # At application startup
    global_session = Session()  # BAD: Created once, used forever!

    def get_products():
        # Using the same session for every request
        return global_session.query(Product).all()

    # Problems:
    # - Session accumulates objects in identity map (memory leak)
    # - Stale data: changes by other processes aren't visible
    # - Database connections held open unnecessarily
    # - Concurrency issues in multi-threaded applications

    WHY THIS IS BAD:
    - Sessions are designed to be short-lived (per-request)
    - Identity map grows unbounded, consuming memory
    - Stale reads: you see data from when session was created
    - Connection pool exhaustion under load

    HOW TO FIX:
    - Create session per-request or per-unit-of-work
    - Use scoped_session for thread-local sessions
    - Use session context managers: with Session() as session:
    - Close sessions explicitly when done
    """)

    # =========================================================================
    # BAD PATTERN 8: Ignoring the ORM's capabilities
    # =========================================================================
    print("\n--- BAD: Raw string SQL when ORM would work ---")

    # BAD CODE START --------------------------------------------------------
    # Using raw SQL with string concatenation - SQL INJECTION RISK!
    user_input = "TechCorp"  # Imagine this comes from user

    # NEVER DO THIS - SQL Injection vulnerability!
    # bad_query = f"SELECT * FROM manufacturers WHERE name = '{user_input}'"
    # results = session.execute(bad_query)

    # Even with text(), string formatting is dangerous:
    # bad_query = text(f"SELECT * FROM manufacturers WHERE name = '{user_input}'")
    # BAD CODE END ----------------------------------------------------------

    print("""
    WHY THIS IS BAD:
    - SQL injection vulnerability (user_input could be: "'; DROP TABLE users;--")
    - Loses type safety and validation
    - No relationship handling
    - Results aren't ORM objects (no automatic caching, change tracking)
    - Harder to maintain and refactor

    HOW TO FIX:
    - Use ORM query methods: session.query(Manufacturer).filter(Manufacturer.name == user_input)
    - If raw SQL is needed, use bound parameters:
      session.execute(text("SELECT * FROM manufacturers WHERE name = :name"), {"name": user_input})
    """)


# ============================================================================
# PART 5: COMPARING GOOD VS BAD APPROACHES
# ============================================================================

def comparison_demo(session: Session):
    """Side-by-side comparison of good vs bad approaches."""

    print("\n" + "="*70)
    print("SIDE-BY-SIDE COMPARISONS")
    print("="*70)

    # -------------------------------------------------------------------------
    # Comparison 1: Loading related data
    # -------------------------------------------------------------------------
    print("\n--- Comparison: Loading products with manufacturer names ---")

    print("\nBAD WAY (N+1 queries):")
    print("  products = session.query(Product).all()")
    print("  for p in products:")
    print("      print(p.manufacturer.name)  # Query for each product!")

    print("\nGOOD WAY (1-2 queries):")
    print("  products = session.query(Product).options(")
    print("      joinedload(Product.manufacturer)")
    print("  ).all()")
    print("  for p in products:")
    print("      print(p.manufacturer.name)  # No additional queries!")

    # -------------------------------------------------------------------------
    # Comparison 2: Counting records
    # -------------------------------------------------------------------------
    print("\n--- Comparison: Counting expensive products ---")

    print("\nBAD WAY (loads all data):")
    print("  all_products = session.query(Product).all()")
    print("  count = len([p for p in all_products if p.price > 100])")

    print("\nGOOD WAY (database does the counting):")
    print("  count = session.query(func.count(Product.product_id)).filter(")
    print("      Product.price > 100")
    print("  ).scalar()")

    # Actually run both and show the difference
    from sqlalchemy import func

    # Good way
    count = session.query(func.count(Product.product_id)).filter(
        Product.price > 100
    ).scalar()
    print(f"\n  Result: {count} expensive products")

    # -------------------------------------------------------------------------
    # Comparison 3: Getting products by multiple IDs
    # -------------------------------------------------------------------------
    print("\n--- Comparison: Getting products by multiple IDs ---")

    print("\nBAD WAY (query per ID):")
    print("  ids = [1, 2, 3, 4, 5]")
    print("  products = []")
    print("  for id in ids:")
    print("      p = session.query(Product).get(id)")
    print("      products.append(p)")

    print("\nGOOD WAY (single query):")
    print("  ids = [1, 2, 3, 4, 5]")
    print("  products = session.query(Product).filter(")
    print("      Product.product_id.in_(ids)")
    print("  ).all()")


# ============================================================================
# PART 6: ADVANCED ORM FEATURES
# ============================================================================

def advanced_features_demo(session: Session):
    """Demonstrate advanced but useful ORM features."""

    print("\n" + "="*70)
    print("ADVANCED ORM FEATURES")
    print("="*70)

    # -------------------------------------------------------------------------
    # Feature 1: Query debugging with echo
    # -------------------------------------------------------------------------
    print("\n--- Feature: Query debugging with echo ---")
    print("""
    # Enable SQL echo to see all generated queries:
    engine = create_engine('postgresql://localhost/demo_db', echo=True)

    # Or for specific session:
    import logging
    logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)
    """)

    # -------------------------------------------------------------------------
    # Feature 2: Raw SQL when needed (safely!)
    # -------------------------------------------------------------------------
    print("\n--- Feature: Safe raw SQL execution ---")

    from sqlalchemy import text

    # Using bound parameters - SAFE!
    result = session.execute(
        text("SELECT name, price FROM products WHERE price > :min_price"),
        {"min_price": 100}
    ).fetchall()

    print("Products over $100 (via raw SQL with bound params):")
    for name, price in result[:3]:
        print(f"  {name}: ${price}")

    # -------------------------------------------------------------------------
    # Feature 3: Subqueries
    # -------------------------------------------------------------------------
    print("\n--- Feature: Subqueries ---")

    # Subquery to get average price
    avg_price_subq = session.query(
        func.avg(Product.price)
    ).scalar_subquery()

    # Products above average price
    above_avg = session.query(Product).filter(
        Product.price > avg_price_subq
    ).all()

    print(f"Products above average price: {len(above_avg)}")

    # -------------------------------------------------------------------------
    # Feature 4: Window functions (PostgreSQL supports these well!)
    # -------------------------------------------------------------------------
    print("\n--- Feature: Using func for database functions ---")

    # Example: Get products with their rank by price
    ranked = session.query(
        Product.name,
        Product.price,
        func.row_number().over(order_by=Product.price.desc()).label('price_rank')
    ).limit(5).all()

    print("Top 5 products by price:")
    for name, price, rank in ranked:
        print(f"  #{rank}: {name} - ${price}")

    # -------------------------------------------------------------------------
    # Feature 5: PostgreSQL-specific RETURNING with ORM
    # -------------------------------------------------------------------------
    print("\n--- Feature: PostgreSQL RETURNING clause ---")
    print("""
    # PostgreSQL supports RETURNING to get data back from INSERT/UPDATE/DELETE
    # With SQLAlchemy, use the returning() method:

    from sqlalchemy import insert
    stmt = insert(Product).values(name='Test', sku='TEST-001', price=99.99)
    stmt = stmt.returning(Product.product_id, Product.name)
    result = session.execute(stmt)
    new_row = result.fetchone()
    print(f"Inserted product ID: {new_row.product_id}")
    """)


# ============================================================================
# PART 7: ORM PERFORMANCE TIPS SUMMARY
# ============================================================================

def print_performance_tips():
    """Print a summary of ORM performance best practices."""

    print("\n" + "="*70)
    print("ORM PERFORMANCE TIPS SUMMARY")
    print("="*70)

    tips = """
    1. ALWAYS check for N+1 queries
       - Enable SQL logging during development
       - Use eager loading (joinedload, selectinload) for relationships you'll access

    2. Select only what you need
       - Use query(Model.col1, Model.col2) instead of query(Model)
       - Avoid SELECT * patterns

    3. Let the database do the work
       - Use func.count(), func.avg(), func.sum() instead of Python
       - Filter in WHERE clause, not in Python

    4. Batch operations
       - Use bulk_insert_mappings() for mass inserts
       - Use update() with filter() for mass updates
       - Commit in batches, not per-row

    5. Use appropriate loading strategies
       - joinedload(): Good for many-to-one (product->manufacturer)
       - selectinload(): Good for one-to-many (manufacturer->products)
       - subqueryload(): Alternative for one-to-many with complex filters

    6. Mind your sessions
       - Keep sessions short-lived (per request)
       - Don't share sessions between threads
       - Close sessions when done

    7. Index your columns
       - Add indexes for columns used in WHERE, JOIN, ORDER BY
       - ORM doesn't create indexes automatically!

    8. Use EXPLAIN ANALYZE
       - session.query(...).statement to get the SQL
       - Run EXPLAIN ANALYZE on slow queries in psql
       - Look for sequential scans on large tables

    9. Consider query caching
       - dogpile.cache for ORM-level caching
       - Redis/Memcached for frequently accessed data

    10. Profile in production-like conditions
        - Small dev datasets don't reveal scaling issues
        - Test with realistic data volumes
    """
    print(tips)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Run all demonstrations."""

    print("="*70)
    print("SQLAlchemy ORM Demonstration (PostgreSQL)")
    print("="*70)

    try:
        # Setup
        engine = create_database()
        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            # Populate data
            populate_sample_data(session)

            # Run demonstrations
            good_patterns_demo(session)
            bad_patterns_demo(session)
            comparison_demo(session)
            advanced_features_demo(session)
            print_performance_tips()

        finally:
            session.close()

        print("\n" + "="*70)
        print("Demonstration complete!")
        print("="*70)

    except Exception as e:
        print(f"\nError: {e}")
        print("\nMake sure PostgreSQL is running and the database exists:")
        print("  createdb demo_db")
        print("\nOr set DATABASE_URL environment variable:")
        print("  DATABASE_URL=postgresql://user:pass@host/dbname python orm_demo.py")
        raise


if __name__ == '__main__':
    main()
