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
            float
        
        accuracy = ( tp + tn ) / (tp + tn + fp + fn)
        '''
        return (tp + tn) / (tp + tn + fp + fn)
        
    def precision(tp: int, fp: int):
        '''
        Calculate the precision as a ratio of the number of true positives to the total number of positives.
        
        Arguments:
            tp: number of true positives
            fp: number of false positives  
        
        Raises:
            None
        
        Returns:
            float
        
        precision = tp / (tp + fp)
        '''
        return tp / (tp + fp)
    
    def metrics_reports(accuracy: float, precision: float, recall: float, specificity: float):
        '''
        Something here.
        
        Arguments:
            accuracy:       instance of the accuracy object.
            precision:      instance of the precision object.
            recall:         instance of the recall object.
            specificity:    instance of the specificity object.
        
        Raises:
            None
        
        Returns:
            String
        '''
        output = "\nThe performance metrics of the binary classifier presented with image ??? are as follows:\naccuracy = {}\nprecision = {}\nrecall = {}\nspecificity = {}\n".format(accuracy, precision, recall, specificity)

        print(output)

BinaryClassiferMetrics.metrics_reports(0.9, 1.0, 0.8, 0.9)
