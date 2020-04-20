class BinaryClassiferMetrics:
    '''
    '''
    def binary_confusion_matrix():
    '''
    '''
        pass
        
    def accuracy(tp: int, tn: int, fp: int, fn: int):
    '''
    Calculate the accuracy as a ratio of the number of correct predictions to the total number of predictions.
    
    Arguments:
        tp: number of true positives
        tn: number of true negatives
        fp: number of false positives
        fn: number of false negatives
        
    Raises:
        None
    
    Returns:
        Integer
    
    acc = ( tp + tn ) / (tp + tn + fp + fn)
    '''
        return (tp + tn) / (tp + tn + fp + fn)
        
    def precision():
    '''
    Calculate the precision as a ratio of the number of true positives to the total number of positives.
    
    Arguments:
        tp: number of true positives
        fp: number of false positives  
    
    Raises:
        None
    
    Returns:
        Integer
    
    acc = tp / (tp + fp)
    '''
        return tp / (tp + fp)
    
    def area_under_the_curve():
    '''
    '''
        pass
    
    def metrics_reports():
    '''
    '''
        pass
