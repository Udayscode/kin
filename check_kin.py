# check_kin.py
try:
    import pydantic
    import temporalio
    import redis
    print("✅ Core dependencies installed.")
    
    # Test internal import
    from models import schemas
    print("✅ Internal modules linked.")
except ImportError as e:
    print(f"❌ Setup error: {e}")
