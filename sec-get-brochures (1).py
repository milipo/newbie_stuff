
import selenium
from selenium import webdriver 
from selenium.webdriver.support.ui import WebDriverWait
import time 
import PyPDF4
import pandas as pd
from pandas import DataFrame
from selenium.common.exceptions import NoSuchElementException
import os
import shutil
import glob
import sqlite3
import warnings

# excel = pd.read_excel('firmcrdnumbers.xls') 


# In[]:

def scraper(input): 
    
    nbrochures = list()
    global browser
    
    for firm in input:
        
        url = 'https://adviserinfo.sec.gov/firm/brochure/'+firm
                
        browser.get(url)
        time.sleep(4)

        all_options = browser.find_elements_by_class_name("link-nostyle.ng-binding")
        firm_brochures = len(all_options)
        
        # If no brochures, we're done 
        if firm_brochures == 0:
            nbrochures.append(0)
            time.sleep(3)
            continue
 
        try: 
            for option in all_options:
                option.click()
                time.sleep(5)
        except selenium.common.exceptions.ElementClickInterceptedException:
            # Get the layer away
            element = browser.find_element_by_link_text('No, thanks')
            element.click() 
            time.sleep(1)
            
            # Try again 
            for option in all_options:
                option.click()
                time.sleep(5)
        except Exception as e:
            # Other error 
            print(e)
            
        # Convert PDFs to text 
        files = os.listdir('source-data\\SEC\\brochures\\pdf\\')
        pdfs = []  
        for file in files:
            if file.endswith('.pdf'):
                pdfs.append(file)
                
        k=0
        for pdf in pdfs:
            k += 1
            
            # Load PDF
            thispdf = open('source-data\\SEC\\brochures\\pdf\\'+pdf, 'rb')
            
            # Initialize PDF reader
            pdfReader = PyPDF4.PdfFileReader(thispdf)  
            thistext = '' 
            
            try:
                # Will fail if PDF is encrypted
                pages = pdfReader.numPages
            except:
                # Need to decrement number of good brochures
                firm_brochures -= 1
                # On to the next
                thispdf.close()
                os.remove('source-data\\SEC\\brochures\\pdf\\'+pdf)
                continue
            
            for page in range(pages):
                try:
                    thispage = pdfReader.getPage(page).extractText()
                    thistext = thistext+thispage+' '
                except:
                    pass
            
            # Close PDF 
            thispdf.close()
            
            # Strip line breaks
            thistext = thistext.replace('\n','')
            
            # See if text got processed  
            if len(thistext) > 100:           
                # Write text file
                thisoutput = open('source-data\\SEC\\brochures\\txt\\'+firm+'-'+str(k)+'.txt','w+',encoding='utf-8')
                thisoutput.writelines(thistext)
                thisoutput.close()
            else:
                # Need to decrement number of good brochures
                firm_brochures -= 1

            # Delete PDF 
            os.remove('source-data\\SEC\\brochures\\pdf\\'+pdf)
            print('.', end="") 
        
        # Record number of brochures 
        nbrochures.append(firm_brochures)
        print('*', end="") 
        
    return nbrochures 

# In[]:

# Get CRDs table 
conn = sqlite3.connect('db\\store.db')
crds = pd.read_sql_query("SELECT firmcrdnb from SECFIRMS", conn) 
conn.close()

# Prepare output
crds['nbrochures'] = 0

input = list(crds['firmcrdnb'])

# Set up headers
mime_types = "application/pdf,application/vnd.adobe.xfdf,application/vnd.fdf,application/vnd.adobe.xdp+xml"

fp = webdriver.FirefoxProfile()
fp.set_preference("http.response.timeout",8)
fp.set_preference("dom.max_script_run_time",8)
fp.set_preference("browser.download.defaultFolder", 'C:\\source-data\\SEC\\brochures\\pdf\\');
fp.set_preference("browser.download.folderList", 2)
fp.set_preference("browser.download.manager.showWhenStarting", False)
fp.set_preference("browser.download.dir", 'C:\\source-data\\SEC\\brochures\\pdf\\')
fp.set_preference("browser.helperApps.neverAsk.saveToDisk", mime_types)
fp.set_preference("plugin.disable_full_page_plugin_for_types", mime_types)
fp.set_preference("pdfjs.disabled", True)

browser = webdriver.Firefox(firefox_profile=fp)
warnings.filterwarnings("ignore")


# In[]:

# Prepare inputs
crdbegin = 501
crdend   = 600
crdinput = input[crdbegin:crdend]

brochurecounts = scraper(crdinput)

# Record brochure counts
j = 0
for i in range(crdbegin, crdend):
    crds.at[i, 'nbrochures'] = brochurecounts[j]
    j += 1
del(j)

# Quit selenium
browser.quit()



