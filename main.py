from engine.compute_factors import compute_all_factors
from tasks.annual_tasks import annual_update
from tasks.daily_tasks import daily_update
from tasks.seasonal_tasks import seasonal_update

if __name__ == "__main__":
    compute_all_factors.compute_all_factors()
    #daily_update()
    #seasonal_update()
    #annual_update()