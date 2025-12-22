import pandas as pd
import re
import unicodedata
import spacy
import numpy as np


"""
a dictionary expanding some common contractions greatly inspired by the following list: 
https://github.com/dipanjanS/practical-machine-learning-with-python/blob/master/
notebooks/Ch07_Analyzing_Movie_Reviews_Sentiment/contractions.py 

"""
CONTRACTION_MAP = {
    "ain't": "is not",
    "aren't": "are not",
    "can't": "cannot",
    "cz": "because",
    "could've": "could have",
    "couldn't": "could not",
    "didn't": "did not",
    "doesn't": "does not",
    "don't": "do not",
    "gonna": "going to",
    "hadn't": "had not",
    "hasn't": "has not",
    "haven't": "have not",
    "he'd": "he would",
    "he'll": "he will",
    "he's": "he is",
    "how'll": "how will",
    "how's": "how is",
    "I'd": "I would",
    "I'll": "I will",
    "I'm": "I am",
    "I've": "I have",
    "i'd": "i would",
    "i'll": "i will",
    "i'm": "i am",
    "i've": "i have",
    "isn't": "is not",
    "it'd": "it would",
    "it'll": "it will",
    "it's": "it is",
    "let's": "let us",
    "ma'am": "madam",
    "mayn't": "may not",
    "might've": "might have",
    "must've": "must have",
    "mustn't": "must not",
    "needn't": "need not",
    "o'clock": "of the clock",
    "oughtn't": "ought not",
    "oughtn't've": "ought not have",
    "shan't": "shall not",
    "sha'n't": "shall not",
    "she'd": "she would",
    "she'd've": "she would have",
    "she'll": "she will",
    "she's": "she is",
    "should've": "should have",
    "shouldn't": "should not",
    "shouldn't've": "should not have",
    "so've": "so have",
    "so's": "so as",
    "that'd": "that would",
    "that's": "that is",
    "there'd": "there would",
    "there'd've": "there would have",
    "there's": "there is",
    "they'd": "they would",
    "they'll": "they will",
    "they'll've": "they will have",
    "they're": "they are",
    "they've": "they have",
    "wanna": "want to",
    "wasn't": "was not",
    "we'd": "we would",
    "we'll": "we will",
    "we'll've": "we will have",
    "we're": "we are",
    "we've": "we have",
    "weren't": "were not",
    "what'll": "what will",
    "what'll've": "what will have",
    "what're": "what are",
    "what's": "what is",
    "what've": "what have",
    "when's": "when is",
    "when've": "when have",
    "where'd": "where did",
    "where's": "where is",
    "where've": "where have",
    "who'll": "who will",
    "who's": "who is",
    "who've": "who have",
    "why's": "why is",
    "why've": "why have",
    "will've": "will have",
    "won't": "will not",
    "won't've": "will not have",
    "would've": "would have",
    "wouldn't": "would not",
    "wouldn't've": "would not have",
    "y'all": "you all",
    "y'all're": "you all are",
    "you'd": "you would",
    "you'd've": "you would have",
    "you'll": "you will",
    "you'll've": "you will have",
    "you're": "you are",
    "you've": "you have"
}



def remove_special_char(text, special_characters = ['~','@', '#', '$', '%','^', '&', '*'], numeric = False):
    """
    This function cleans text from any special characters.
    You can add additional input as a list of special characters you want to remove.
    Or you can decide on removing all types of characters except letters and numbers.
    You can also add an additional boolean input that indicates if the user wants to 
    remove the numbers as well.

    Parameters
    -----------
    text: str

    Returns
    ---------
    filtered_text: str

    Examples
    --------
    >>> remove_special_char('processing$##')
    'processing'

    """
    pattern = '[' + special_characters[0]
    for char in special_characters:
        pattern = pattern + '|' + char
    if (numeric):
        pattern = pattern + '|'+ '0-9'
    pattern = pattern + ']'
    #remove them
    filtered_text = re.sub(pattern, r'', text)
    return filtered_text


def remove_accents(text):
    """
    This function removes accent from text

    Parameters
    ----------
    text: str

    Returns
    --------
    filtered_text: str
        The text without the accents

    Examples
    --------
    >>> remove_special_char('déjà vu')
    'deja vu'

    """
    filtered_text = unicodedata.normalize(
        'NFKD', text).encode('ascii', 'ignore').decode('utf8')

    return filtered_text


def expand_contractions(text):
    """
    This function expands the contractions in text

    Parameters
    ----------
    text: str

    Returns
    --------
    filtered_text: str
        The text with expanded contractions

    Examples
    --------
    >>> remove_special_char('It couldn't be better')
    'It could not be better'

    """
    text = " ".join([CONTRACTION_MAP[word] if word in CONTRACTION_MAP else word for word in text.split()])
    return text


def remove_stopwords_punctuation(text, lang_model, lemmatizing=False, stop_words=False):
    """
    This function uses spacy to remove stop_words and punctuation marks.
    It can also replace words with their lemma.

    Parameters
    -----------
    text: str
    lang_model: spacy Language object
    lemmatizing: boolean
    stop_words: boolean
        True if it is desired to remove stop_words

    Returns
    ---------
    filtered_text:str

    Examples
    --------
    >>> lang_model = spacy.load("en_core_web_sm")
    >>> remove_stopwords_punctuation('Those were amazing days!!!', lang_model, lemmatizing=False, stop_words=False)
    'Those were amazing days'

    >>> remove_stopwords_punctuation('Those were amazing days!!!', lang_model, lemmatizing=True, stop_words=False)
    'Those be amazing day'

    >>> remove_stopwords_punctuation('Those were amazing days!!!', lang_model, lemmatizing=True, stop_words=True)
    'amazing day'

    """

    doc_text = lang_model(text)
   
    if lemmatizing:
        st= " ".join([token.lemma_ for token in doc_text if not(token.is_punct)])
    else:
        st= " ".join([token.text for token in doc_text if not(token.is_punct)])
    return st

  

    return filtered_text


def preprocess_text(text, nlp, special_characters = ['~','@', '#', '$', '%', '^', '&', '*'], numeric = False, lemmatizing=False):
    """
    This function pre-processes the text.

    Parameters
    -----------
    text: str
    lang_model: spacy Language object
    lemmatizing: boolean
    stop_words: boolean
        True if it is desired to remove stop_words

    Returns
    --------
    filtered_text:str


    Examples
    --------
    >>> lang_model = spacy.load("en_core_web_sm")
    >>> preprocess_text("\n\n\nHey that's a $$great news!!", lang_model, lemmatizing=True, stop_words=False)
    'hey that be a great news'

    >>> preprocess_text("\n\n\nHey that's a $$great news!!", lang_model, lemmatizing=True, stop_words=True)
    'hey great news'

    """

    # remove special characters
    text =  remove_special_char(text, special_characters, numeric)

    # convert text to lower case (you can use lower() function)
    # remove extra spaces with strip() function
    text = text.lower().strip()

    # remove accents
    text =  remove_accents(text)

    # expand contractions
    text = expand_contractions(text)

    # remove stop_words amd punctuations
    filtered_text =  remove_stopwords_punctuation(text, nlp, lemmatizing)
   

    return filtered_text
