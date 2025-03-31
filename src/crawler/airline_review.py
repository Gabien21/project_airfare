from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
from bs4 import BeautifulSoup
import os



def retrieve_general_data(page_content): 
    soup = BeautifulSoup(page_content, "html.parser")

    time.sleep(3)
    name = soup.find('div', class_="jIkPg G u")

    phone = soup.find('div', class_="bOZAZ VNlYD")

    address = soup.find('div', class_="biGQs _P pZUbB W hmDzD")


    link_element = soup.find('div', class_="jWfod u")

    


    average_rating = soup.find('span', class_="ammfn")

    attributes = {}
    table = soup.find('div', class_ = "lVVcb")
    attribute_elements = table.find_all('div')
    for ele in attribute_elements: 
        name_attribute = (ele.find('span',class_='exvvN').text)
        rate = (ele.find('span',class_='RkhSR').text)
        attributes[name_attribute]=  rate


    total_review = soup.find('div', class_="biGQs _P fiohW uuBRH")



    total_ratings = {}
    rating_table = soup.find('div', class_="AugPH w u")

    rating_elements = rating_table.find_all('div',class_="jxnKb")
    for rating_element in rating_elements: 
        name_rating = (rating_element.find('div',class_='Ygqck o W q').text)
        count = (rating_element.find('div',class_='biGQs _P fiohW biKBZ osNWb').text)
        total_ratings[name_rating]=  count


    popular_mentions_elements = soup.find('div', class_="TuqGj")
    popular_mentions = []
    for element in popular_mentions_elements.find_all('span',class_="_T"): 
        popular_mentions.append(element.text)

    file_path = 'data/overall/bamboo_general_info.txt'
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(file_path,"a") as file :
        if name : 
            file.write(f"Name: {name.text}\n")
        else : 
            file.write(f"Name: Not found\n")

        if phone : 
            file.write(f"Phone: {phone.text}\n")
        else : 
            file.write(f"Phone: Not found\n")

        if address : 
            file.write(f"Address: {address.text}\n")
        else : 
            file.write(f"Address: Not found\n")

        if link_element:
            for link in link_element.find_all('a',href=True) :
                if 'http://' in link['href']:  
                    file.write(f"Website: {link['href']}\n")
        else:
            file.write(f"Website: Not found\n")

        if average_rating : 
            file.write(f"Average Rating: {average_rating.text}\n")
        else : 
            file.write(f"Average Rating: Not found\n")

        if total_review : 
            file.write(f"Total review: {total_review.text}\n")
        else : 
            file.write(f"Total review: Not found\n")

        file.write(f"Popular mention: [")
        for popular_mention in popular_mentions[:-1]:
            file.write(f"{popular_mention}, ")
        file.write(f"{popular_mentions[-1]}")
        file.write(f"]\n")



        file.write("Attributes: {")
        for i, key in enumerate(attributes.keys()):
            if i < len(attributes) - 1:
                file.write(f"'{key}': '{attributes[key]}', ")
            else:
                file.write(f"'{key}': '{attributes[key]}'")
        file.write("}\n")


        file.write("Total rating: {")
        for i, key in enumerate(total_ratings.keys()):
            if i < len(total_ratings) - 1:
                file.write(f"'{key}': '{total_ratings[key]}', ")
            else:
                file.write(f"'{key}': '{total_ratings[key]}'")
        file.write("}\n")


    


def retrive_review_data(page_content) : 
    soup = BeautifulSoup(page_content, "html.parser")
    
    reviews_data = []


    review_elements = soup.find_all('div',class_="lwGaE A")
    for review_element in review_elements:
        rating = review_element.find("svg",class_="UctUV d H0").text
        title = review_element.find("div",class_ = "biGQs _P fiohW uuBRH").text
        full_review = review_element.find("span",class_ = "JguWG").text
        information_ele = review_element.find_all("div",class_ = "biGQs _P pZUbB hmDzD")
        if len(information_ele) > 1 : 
            information = information_ele[1].text
        else : 
            information = None
        review_dict = {
            "Rating": rating,
            "Title": title,
            "Full Review": full_review,
            "Information": information,
        }

        service_rating_table = review_element.find('div',class_= "JxiyB f")
        if service_rating_table is not None:
            elements = service_rating_table.find_all('div',class_="msVPq")
            service_ratings = []
            for element in elements: 
                service_rating = element.find('svg', class_='UctUV d H0').text
                service_info = element.find('div', class_='biGQs _P pZUbB osNWb').text
                service_ratings.append({"Service Rating": service_rating, "Service Info": service_info})
            review_dict["Service Ratings"] = service_ratings
        else :
            review_dict["Service Ratings"] = None

        reviews_data.append(review_dict)
    return reviews_data



def get_all_review_page(page_content) : 
    all_page_review_data = []
    browser.execute_script("window.scrollTo(0, document.body.scrollHeight)")
    page_1_review = retrive_review_data(page_content)


    all_page_review_data.extend(page_1_review)

    while True :
        try: 
            next_button = browser.find_element(By.XPATH, "//button[@aria-label='Next page']")
        except Exception as e :
            next_button = None
            print(e)
        if next_button is None : 
            print("There is no button")
            break
        print(next_button)
        browser.execute_script("arguments[0].scrollIntoView(true);", next_button)
        next_button.click()
        print("There is new page")
        time.sleep(4)
        
        updated_html_content = browser.page_source
        next_review_data = retrive_review_data(updated_html_content)
        all_page_review_data.extend(next_review_data)

        print(f"Total reviews collected: {len(all_page_review_data)}")

    df = pd.DataFrame(all_page_review_data)


    df.to_csv("data/review/bamboo_all_reviews_data.csv", index=False, encoding="utf-8")

    print("All reviews have been saved to 'bamboo_all_reviews_data.csv'.")

if __name__ == "__main__" : 
    driver_path = "/usr/local/bin/chromedriver"

    options = Options()

    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    service = Service(driver_path)
    browser = webdriver.Chrome(service=service, options=options)

    # url = "https://www.tripadvisor.com/Airline_Review-d8728891-Reviews-VietJetAir"
    # url = "https://www.tripadvisor.com/Airline_Review-d8729180-Reviews-Vietnam-Airlines"
    url = "https://www.tripadvisor.com/Airline_Review-d17550096-Reviews-Bamboo-Airways"
    browser.get(url)


    time.sleep(5)  


    html_content = browser.page_source
    retrieve_general_data(html_content)
    get_all_review_page(html_content)

    time.sleep(5)

    browser.quit()