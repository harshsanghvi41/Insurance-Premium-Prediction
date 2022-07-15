from premium.pipeline.pipeline import Pipeline
from premium.exception import PremiumException
from premium.logger import logging

def main():
    try:
        pipeline = Pipeline()
        pipeline.run_pipeline()

    except Exception as e:
        logging.error(f"{e}")
        print(e)

if __name__ == "__main__":
    main()