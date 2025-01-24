from include import tap_spacex 

def main():
    based_url="https://api.spacexdata.com/v4/"
    
    #tap_spacex.fetch_launches(based_url)
    #tap_spacex.fetch_rockets(based_url)
    #tap_spacex.fetch_capsules(based_url)
    #tap_spacex.fetch_company(based_url)
    #tap_spacex.fetch_cores(based_url)
    #tap_spacex.fetch_crew(based_url)
    #tap_spacex.fetch_dragons(based_url)
    #tap_spacex.fetch_landpads(based_url)
    #tap_spacex.fetch_payloads(based_url)
    #tap_spacex.fetch_roadster(based_url)
    #tap_spacex.fetch_history(based_url)
    #tap_spacex.fetch_launchpads(based_url)
    tap_spacex.fetch_ships(based_url)
    


if __name__ == "__main__":
    main()
