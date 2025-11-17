import requests
import json
import os
from dotenv import load_dotenv
import time
import pymysql.cursors
import citizenphil as cp
from datetime import datetime
from bs4 import BeautifulSoup, NavigableString, Tag
#import re

# Load .env file 
load_dotenv()

strwikipediauseragent = os.getenv("WIKIMEDIA_USER_AGENT")
print("strwikipediauseragent",strwikipediauseragent)
headers = {
    'User-Agent': strwikipediauseragent
}

def get_linked_pages(wikidata_id, strprops, strlanguage):
    url = f"https://www.wikidata.org/w/api.php"
    if strprops == '':
        params = {
            'action': 'wbgetentities',
            'format': 'json',
            'ids': wikidata_id,
            'languages': strlanguage
        }
    else:
        params = {
            'action': 'wbgetentities',
            'format': 'json',
            'ids': wikidata_id,
            'props': strprops,
            'languages': strlanguage
        }
    time.sleep(0.1)
    response = requests.get(url, params=params, headers=headers)
    print(response)
    if response.status_code == 200:
        data = response.json()
        return data
        # entities = data.get('entities', {})
        # sitelinks = entities.get(wikidata_id, {}).get('sitelinks', {})
        # linked_pages = {site: sitelinks[site]['title'] for site in sitelinks}
        # return linked_pages
    else:
        return f"Error: {response.status_code}"

def extract_titles_and_text(html_content):
    # V4
    soup = BeautifulSoup(html_content, 'html.parser')
    # Find all h2 headers
    headers = soup.find_all('h2')
    # Initialize result array
    result = []
    # Extract text before the first h2 header
    first_h2 = headers[0] if headers else None
    # Find the first h2 and extract all text before it
    section_text = ""
    for sibling in soup.body.find_all(recursive=True):
        if sibling == first_h2:
            break
        elif sibling.name == "h2":
            break
        elif sibling.name == 'p':
            text = sibling.get_text()
            if text:
                section_text += '\n' + text + " "
        elif sibling.name == 'h3' or sibling.name == 'h4':
            text = sibling.get_text()
            if text:
                section_text += '\n' + text + " "
        elif sibling.name == 'ul':
            for li in sibling.find_all('li', recursive=True):
                section_text += '\n- ' + ' '.join(t.strip() for t in li.strings if t.strip())
        elif sibling.name == 'ol':
            for li in sibling.find_all('li', recursive=True):
                section_text += '\n- ' + ' '.join(t.strip() for t in li.strings if t.strip())
        elif sibling.name == 'ul' and 'gallery' in sibling.get('class', []):
            caption = sibling.find('li', class_='gallerycaption')
            if caption:
                section_text += '\n' + caption.get_text() + " "
            for gallery_text in sibling.find_all('div', class_='gallerytext'):
                text = gallery_text.get_text()
                if text:
                    section_text += '\n' + text
    section_text = section_text.strip()
    while "\n\n" in section_text:
        section_text = section_text.replace("\n\n", "\n")
    result.append(('Intro',section_text))
    
    # Extract text for each h2 section
    for h2 in headers:
        title = h2.get_text()
        section_text = ""
        for sibling in h2.find_all_next():
            if sibling.name == "h2":
                break
            elif sibling.name == 'p':
                text = sibling.get_text()
                if text:
                    section_text += '\n' + sibling.get_text() + " "
            elif sibling.name == 'h3' or sibling.name == 'h4':
                text = sibling.get_text()
                if text:
                    section_text += '\n' + text + " "
            elif sibling.name == 'ul':
                for li in sibling.find_all('li', recursive=True):
                    section_text += '\n- ' + ' '.join(t.strip() for t in li.strings if t.strip())
            elif sibling.name == 'ol':
                for li in sibling.find_all('li', recursive=True):
                    section_text += '\n- ' + ' '.join(t.strip() for t in li.strings if t.strip())
            elif sibling.name == 'ul' and 'gallery' in sibling.get('class', []):
                caption = sibling.find('li', class_='gallerycaption')
                if caption:
                    section_text += '\n' + caption.get_text() + " "
                for gallery_text in sibling.find_all('div', class_='gallerytext'):
                    text = gallery_text.get_text()
                    if text:
                        section_text += '\n' + text
        section_text = section_text.strip()
        while "\n\n" in section_text:
            section_text = section_text.replace("\n\n", "\n")
        result.append((title, section_text))
    return result

cwd = os.getcwd()

# wikidata_id = "Q24829" # Wikidata ID for Orson Welles

#strlanguage = 'en'
strlanguage = 'fr'
strprops = 'sitelinks'

strprocessesexecutedprevious = cp.f_getservervariable("strwikipediacrawlerprocessesexecuted",0)
strprocessesexecuteddesc = "List of processes executed in the Wikipedia crawler for Wikipedia pages retrieval"
cp.f_setservervariable("strwikipediacrawlerprocessesexecutedprevious",strprocessesexecutedprevious,strprocessesexecuteddesc + " (previous execution)",0)
strprocessesexecuted = ""
cp.f_setservervariable("strwikipediacrawlerprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)

# Connect to the database
connection = pymysql.connect(host=cp.strdbhost, user=cp.strdbuser, password=cp.strdbpassword, database=cp.strdbname, cursorclass=pymysql.cursors.DictCursor)

try:
    with connection:
        with connection.cursor() as cursor:
            cursor2 = cp.connectioncp.cursor()
            # Start timing the script execution
            start_time = time.time()
            strcurrentprocessdesc = "Current process in the Wikipedia crawler for Wikipedia pages retrieval"
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            cp.f_setservervariable("strwikipediacrawlerstartdatetime",strnow,"Date and time of the last start of the Wikipedia crawler",0)
            strtotalruntimedesc = "Total runtime of the Wikipedia crawler"
            strtotalruntimeprevious = cp.f_getservervariable("strwikipediacrawlertotalruntime",0)
            cp.f_setservervariable("strwikipediacrawlertotalruntimeprevious",strtotalruntimeprevious,strtotalruntimedesc + " (previous execution)",0)
            strtotalruntime = ""
            cp.f_setservervariable("strwikipediacrawlertotalruntime",strtotalruntime,strtotalruntimedesc,0)

            arrprocessscope = {201: 'movie', 202: 'person', 203: 'item', 204: 'serie', 209: 'other'}
            
            strmovieidold = cp.f_getservervariable("strwikipediacrawlermovieid",0)
            strpersonidold = cp.f_getservervariable("strwikipediacrawlerpersonid",0)
            stritemidold = cp.f_getservervariable("strwikipediacrawleritemid",0)
            strserieidold = cp.f_getservervariable("strwikipediacrawlerserieid",0)
            strcurrentcontent = cp.f_getservervariable("strwikipediacrawlercurrentcontent",0)
            
            if strcurrentcontent == "person":
                arrprocessscope = {202: 'person', 203: 'item', 204: 'serie', 209: 'other'}
            if strcurrentcontent == "item":
                arrprocessscope = {203: 'item', 204: 'serie', 209: 'other'}
            if strcurrentcontent == "serie":
                arrprocessscope = {204: 'serie', 209: 'other'}
            if strcurrentcontent == "other":
                arrprocessscope = {209: 'other'}
            #arrprocessscope = {201: 'movie'}
            #arrprocessscope = {202: 'person'}
            #arrprocessscope = {203: 'item'}
            #arrprocessscope = {203: 'serie'}
            #arrprocessscope = {209: 'other'}
            for intindex, strcontent in arrprocessscope.items():
                strcurrentprocess = f"{intindex}: processing Wikipedia English and French " + strcontent + " content"
                strprocessesexecuted += str(intindex) + ", "
                cp.f_setservervariable("strwikipediacrawlerprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)
                if intindex == 201:
                    # Processing movies
                    strsql = ""
                    strsql += "SELECT DISTINCT T_WC_TMDB_MOVIE.ID_MOVIE AS id, ID_WIKIDATA FROM T_WC_TMDB_MOVIE "
                    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
                    #strsql += "AND ID_WIKIDATA = 'Q103474' "
                    #strsql += "AND ID_MOVIE >= 1440000 "
                    if strmovieidold != "":
                        strsql += "AND ID_MOVIE >= " + strmovieidold + " "
                    #strsql += "AND TIM_WIKIPEDIA_COMPLETED IS NULL "
                    #strsql += "ORDER BY POPULARITY DESC "
                    strsql += "ORDER BY ID_MOVIE ASC "
                    #strsql += "LIMIT 10 "
                elif intindex == 202:
                    # Processing persons
                    strsql = ""
                    strsql += "SELECT DISTINCT T_WC_TMDB_PERSON.ID_PERSON AS id, ID_WIKIDATA FROM T_WC_TMDB_PERSON "
                    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
                    #strsql += "AND ID_PERSON >= 1225829 "
                    if strpersonidold != "":
                        strsql += "AND ID_PERSON >= " + strpersonidold + " "
                    #strsql += "AND TIM_WIKIPEDIA_COMPLETED IS NULL "
                    #strsql += "ORDER BY POPULARITY DESC "
                    strsql += "ORDER BY ID_PERSON ASC "
                    #strsql += "LIMIT 10 "
                elif intindex == 203:
                    # Processing items
                    strsql = ""
                    strsql += "SELECT DISTINCT T_WC_WIKIDATA_ITEM.ID_WIKIDATA AS id, ID_WIKIDATA FROM T_WC_WIKIDATA_ITEM "
                    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
                    strsql += "AND ID_WIKIDATA NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_MOVIE) "
                    strsql += "AND ID_WIKIDATA NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_PERSON) "
                    if stritemidold != "":
                        strsql += "AND ID_WIKIDATA >= '" + stritemidold + "' "
                    strsql += "ORDER BY ID_WIKIDATA ASC "
                    #strsql += "LIMIT 10 "
                elif intindex == 204:
                    # Processing series
                    strsql = ""
                    strsql += "SELECT DISTINCT T_WC_TMDB_SERIE.ID_SERIE AS id, ID_WIKIDATA FROM T_WC_TMDB_SERIE "
                    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
                    if strserieidold != "":
                        strsql += "AND ID_SERIE >= " + strserieidold + " "
                    #strsql += "AND TIM_WIKIPEDIA_COMPLETED IS NULL "
                    #strsql += "ORDER BY POPULARITY DESC "
                    strsql += "ORDER BY ID_SERIE ASC "
                    #strsql += "LIMIT 100 "
                elif intindex == 209:
                    # Processing other
                    strsql = ""
                    strsql += "SELECT DISTINCT 0 AS id, 'Q1204187' AS ID_WIKIDATA FROM DUAL "
                    #strsql += "UNION ALL "
                    #strsql += "SELECT 1, 'Q1204187' "
                if strsql != "":
                    print(strcurrentprocess)
                    cp.f_setservervariable("strwikipediacrawlercurrentprocess",strcurrentprocess,"Current process in the Wikipedia crawler",0)
                    cp.f_setservervariable("strwikipediacrawlercurrentcontent",strcontent,"Current content processed in the Wikipedia crawler",0)
                    strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                    cp.f_setservervariable("strwikipediacrawler"+strcontent+"startdatetime",strnow,"Date and time of the last start of the Wikipedia crawler for "+strcontent,0)
                    print(strsql)
                    #cp.f_setservervariable("strwikipediacrawlercurrentsql",strsql,"Current SQL query in the Wikipedia crawler",0)
                    cursor.execute(strsql)
                    lngrowcount = cursor.rowcount
                    print(f"{lngrowcount} lines for "+strcontent)
                    lngfrcount = 0
                    lngencount = 0
                    # Fetching all rows from the last executed statement
                    results = cursor.fetchall()
                    # Iterating through the results and printing
                    for row in results:
                        # print("------------------------------------------")
                        lngid = row['id']
                        wikidata_id = row['ID_WIKIDATA']
                        print(f"TMDb {strcontent} id {lngid} Wikidata id: {wikidata_id} ")
                        arrlang = {1: 'en', 2:'fr'}
                        for intlang, strlanguage in arrlang.items():
                            page_content = get_linked_pages(wikidata_id, strprops, strlanguage)
                            if page_content:
                                print(page_content)
                                if 'entities' in page_content:
                                    if wikidata_id in page_content['entities']:
                                        if strprops in page_content['entities'][wikidata_id]:
                                            strkey = strlanguage + 'wiki'
                                            if strkey in page_content['entities'][wikidata_id][strprops]:
                                                page_title = page_content['entities'][wikidata_id][strprops][strkey]['title']
                                                print(f"{strlanguage}: {page_title}")
                                                if page_title:
                                                    url = f'https://{strlanguage}.wikipedia.org/w/api.php'
                                                    params = {
                                                        'action': 'parse',
                                                        'page': page_title,
                                                        'prop': 'text',
                                                        'formatversion': 2,
                                                        'format': 'json'
                                                    }
                                                    # By default we assume no success
                                                    intsuccess = False
                                                    print(url)
                                                    try:
                                                        response = requests.get(url, params=params, headers=headers)
                                                        intsuccess = True
                                                    except requests.exceptions.HTTPError as http_err:
                                                        print(f'HTTP error occurred: {http_err}')  # Handle specific HTTP errors
                                                    except requests.exceptions.ConnectionError as conn_err:
                                                        print(f'Connection error occurred: {conn_err}')  # Handle connection errors
                                                    except requests.exceptions.Timeout as timeout_err:
                                                        print(f'Timeout error occurred: {timeout_err}')  # Handle timeout errors
                                                    except requests.exceptions.RequestException as req_err:
                                                        print(f'Request error occurred: {req_err}')  # Handle other request-related errors
                                                    except Exception as err:
                                                        print(f'An error occurred: {err}')  # Handle any other exceptions
                                                    if intsuccess:
                                                        data = response.json()
                                                        """
                                                        strjsonfilename = wikidata_id + '-' + strlanguage + '-data.json'
                                                        file_path = os.path.join(cwd, strjsonfilename)
                                                        with open(file_path, 'w') as file:
                                                            file.write(json.dumps(data, ensure_ascii=False))
                                                        print(f'File created: {file_path}')
                                                        """
                                                        wikipedia_page_content = data['parse']['text']
                                                        if wikipedia_page_content:
                                                            # print(wikipedia_page_content)
                                                            """
                                                            strpagetitle = page_title.replace(" ","_")
                                                            strfilename = strcontent + "/" + strlanguage + "/" + wikidata_id + '.html'
                                                            file_path = os.path.join(cwd, strfilename)
                                                            with open(file_path, 'w') as file:
                                                                file.write(wikipedia_page_content)
                                                            #print(f'File created: {file_path}')
                                                            """
                                                            if strlanguage == "en":
                                                                lngencount += 1
                                                                cp.f_setservervariable("strwikipediacrawler"+strcontent+"englishcount",str(lngencount),"Count of Wikipedia English pages retrieved for "+strcontent,0)
                                                            if strlanguage == "fr":
                                                                lngfrcount += 1
                                                                cp.f_setservervariable("strwikipediacrawler"+strcontent+"frenchcount",str(lngfrcount),"Count of Wikipedia French pages retrieved for "+strcontent,0)
                                                            wikipedia_page_content = "<body>" + wikipedia_page_content + "</body>"
                                                            arrcontent = extract_titles_and_text(wikipedia_page_content)
                                                            #print(arrcontent)
                                                            lngdisplayorder = 0
                                                            for i, (strsectiontitle, strsectioncontent) in enumerate(arrcontent):
                                                                print(f"{i}. Title: {strsectiontitle}")
                                                                strsectioncontent = strsectioncontent.replace("[edit]","")
                                                                if len(strsectiontitle) > 300:
                                                                    strsectiontitle = strsectiontitle[:300]
                                                                #print(f"   Text: {strsectioncontent}\n")
                                                                lngdisplayorder += 1
                                                                arrcouples = {}
                                                                arrcouples["ID_WIKIDATA"] = wikidata_id
                                                                arrcouples["LANG"] = strlanguage
                                                                arrcouples["ITEM_TYPE"] = strcontent
                                                                arrcouples["DISPLAY_ORDER"] = lngdisplayorder
                                                                arrcouples["TITLE"] = strsectiontitle
                                                                arrcouples["CONTENT"] = strsectioncontent
                                                                strsqltablename = "T_WC_WIKIPEDIA_PAGE_LANG_SECTION"
                                                                strsqlupdatecondition = f"ID_WIKIDATA = '{wikidata_id}' AND LANG = '{strlanguage}' AND DISPLAY_ORDER = {lngdisplayorder}"
                                                                cp.f_sqlupdatearray(strsqltablename,arrcouples,strsqlupdatecondition,1)
                                                                strsqldelete = f"DELETE FROM {strsqltablename} WHERE ID_WIKIDATA = '{wikidata_id}' AND LANG = '{strlanguage}' AND DISPLAY_ORDER > {lngdisplayorder}"
                                                                # print(f"{strsqldelete}")
                                                                cursor2.execute(strsqldelete)
                                                                cp.connectioncp.commit()
                                                                
                                                                # Extract Format data from movie, fr, Fiche Technique section
                                                                if intindex == 201:
                                                                    # This is a movie
                                                                    if strlanguage == "fr":
                                                                        # In French
                                                                        if strsectiontitle == "Fiche technique":
                                                                            # Fiche technique
                                                                            strstringbegin = "\n- Format"
                                                                            strstringend = "\n- "
                                                                            strformatline = ""
                                                                            lngbeginindex = strsectioncontent.find(strstringbegin)
                                                                            if lngbeginindex == -1:
                                                                                strstringbegin = "- Format"
                                                                                lngbeginindex = strsectioncontent.find(strstringbegin)
                                                                            if lngbeginindex != -1:
                                                                                # Begin string found
                                                                                lngbeginindex += len(strstringbegin)
                                                                                lngendindex = strsectioncontent.find(strstringend, lngbeginindex)
                                                                                if lngendindex != -1:
                                                                                    strformatline = strsectioncontent[lngbeginindex:lngendindex].strip()
                                                                                else:
                                                                                    strformatline = strsectioncontent[lngbeginindex:].strip()
                                                                            if strformatline != "":
                                                                                if strformatline[0:2] == ": ":
                                                                                    strformatline = strformatline[2:]
                                                                            print("Format :",strformatline)
                                                                            arrcouples = {}
                                                                            arrcouples["WIKIPEDIA_FORMAT_LINE"] = strformatline
                                                                            arrcouples["DAT_WIKIPEDIA_FORMAT_LINE"] = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                                                                            strsqltablename = "T_WC_TMDB_MOVIE"
                                                                            strsqlupdatecondition = f"ID_MOVIE = {lngid}"
                                                                            cp.f_sqlupdatearray(strsqltablename,arrcouples,strsqlupdatecondition,1)
                                                        # print(wikipedia_page_content)
                                                else:
                                                    print(f'No Wikipedia page found for {strcontent} id {wikidata_id} and language code {strlanguage}')
                        #cp.f_tmdbmoviesetwikipediacompleted(lngid)
                        cp.f_setservervariable("strwikipediacrawler"+strcontent+"wikidataid",wikidata_id,"Current wikidata id in the Wikipedia crawler for "+strcontent,0)
                        cp.f_setservervariable("strwikipediacrawler"+strcontent+"id",str(lngid),"Current id in the Wikipedia crawler for "+strcontent,0)
                        
                    strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                    cp.f_setservervariable("strwikipediacrawler"+strcontent+"enddatetime",strnow,"Date and time of the last end of the Wikipedia crawler for "+strcontent,0)
                    cp.f_setservervariable("strwikipediacrawler"+strcontent+"id","","Current id in the Wikipedia crawler for "+strcontent,0)
                    # Define what is the next content to process
                    if strcontent == "movie":
                        cp.f_setservervariable("strwikipediacrawlercurrentcontent","person","Current content processed in the Wikipedia crawler",0)
                    elif strcontent == "person":
                        cp.f_setservervariable("strwikipediacrawlercurrentcontent","item","Current content processed in the Wikipedia crawler",0)
                    elif strcontent == "item":
                        cp.f_setservervariable("strwikipediacrawlercurrentcontent","serie","Current content processed in the Wikipedia crawler",0)
                    elif strcontent == "serie":
                        cp.f_setservervariable("strwikipediacrawlercurrentcontent","other","Current content processed in the Wikipedia crawler",0)
                    elif strcontent == "other":
                        cp.f_setservervariable("strwikipediacrawlercurrentcontent","","Current content processed in the Wikipedia crawler",0)
            strcurrentprocess = ""
            cp.f_setservervariable("strwikipediacrawlercurrentprocess",strcurrentprocess,strcurrentprocessdesc,0)
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            cp.f_setservervariable("strwikipediacrawlerenddatetime",strnow,"Date and time of the Wikipedia crawler ending",0)
            # Calculate total runtime and convert to readable format
            end_time = time.time()
            strtotalruntime = int(end_time - start_time)  # Total runtime in seconds
            cp.f_setservervariable("strwikipediacrawlertotalruntimesecond",str(strtotalruntime),strtotalruntimedesc,0)
            readable_duration = cp.convert_seconds_to_duration(strtotalruntime)
            cp.f_setservervariable("strwikipediacrawlertotalruntime",readable_duration,strtotalruntimedesc,0)
            print(f"Total runtime: {strtotalruntime} seconds ({readable_duration})")
    print("Process completed")
except pymysql.MySQLError as e:
    print(f"❌ MySQL Error: {e}")
    connection.rollback()
